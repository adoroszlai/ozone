package org.apache.hadoop.ozone.container.replication;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.TarContainerPacker;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_VOLUME_CHOOSING_POLICY;

/**
 * Imports container from tarball.
 */
public class ContainerImporter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerImporter.class);

  public static final String CONTAINER_COPY_DIR = "container-copy";
  private static final String CONTAINER_COPY_TMP_DIR = "tmp";
  private final ContainerSet containerSet;
  private final ContainerController controller;
  private final TarContainerPacker packer;
  private final MutableVolumeSet volumeSet;
  private final VolumeChoosingPolicy volumeChoosingPolicy;
  private final long containerSize;

  public ContainerImporter(ConfigurationSource conf, ContainerSet containerSet,
      ContainerController controller, TarContainerPacker tarContainerPacker,
      MutableVolumeSet volumeSet) {
    this.containerSet = containerSet;
    this.controller = controller;
    this.packer = tarContainerPacker;
    this.volumeSet = volumeSet;
    try {
      volumeChoosingPolicy = conf.getClass(
          HDDS_DATANODE_VOLUME_CHOOSING_POLICY, RoundRobinVolumeChoosingPolicy
              .class, VolumeChoosingPolicy.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    containerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
  }

  public void importContainer(long containerID, Path tarFilePath,
      HddsVolume hddsVolume) throws IOException {

    HddsVolume targetVolume = hddsVolume;
    if (targetVolume == null) {
      targetVolume = chooseNextVolume();
    }
    try {
      KeyValueContainerData containerData;

      try (FileInputStream input = new FileInputStream(tarFilePath.toFile())) {
        byte[] containerDescriptorYaml =
            packer.unpackContainerDescriptor(input);
        containerData = (KeyValueContainerData) ContainerDataYaml
            .readContainer(containerDescriptorYaml);
      }
      containerData.setVolume(targetVolume);

      try (FileInputStream input = new FileInputStream(tarFilePath.toFile())) {
        Container container = controller.importContainer(
            containerData, input, packer);
        containerSet.addContainer(container);
      }
    } finally {
      try {
        Files.delete(tarFilePath);
      } catch (Exception ex) {
        LOG.error("Got exception while deleting temporary container file: "
            + tarFilePath.toAbsolutePath(), ex);
      }
    }
  }

  HddsVolume chooseNextVolume() throws IOException {
    // Choose volume that can hold both container in tmp and dest directory
    return volumeChoosingPolicy.chooseVolume(
        StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList()),
        containerSize * 2);
  }

  public static Path getUntarDirectory(HddsVolume hddsVolume)
      throws IOException {
    return Paths.get(hddsVolume.getVolumeRootDir())
        .resolve(CONTAINER_COPY_TMP_DIR).resolve(CONTAINER_COPY_DIR);
  }
}