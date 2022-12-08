/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.plugin.maven;

import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;
import static org.twdata.maven.mojoexecutor.MojoExecutor.plugin;

import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Goal which stages the Templates into Cloud Storage / Artifact Registry.
 *
 * <p>The process is different for Classic Templates and Flex Templates, please check {@link
 * #stageClassicTemplate(BuildPluginManager, TemplateDefinitions, ImageSpec, String)} and {@link
 * #stageFlexTemplate(TemplateDefinitions, ImageSpec, BuildPluginManager)}, respectively.
 */
@Mojo(
    name = "stage",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplateStageMojo extends TemplateBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateStageMojo.class);

  @Parameter(defaultValue = "${projectId}", readonly = true, required = true)
  protected String projectId;

  @Parameter(defaultValue = "${templateName}", readonly = true, required = false)
  protected String templateName;

  @Parameter(defaultValue = "${bucketName}", readonly = true, required = true)
  protected String bucketName;

  @Parameter(defaultValue = "${stagePrefix}", readonly = true, required = true)
  protected String stagePrefix;

  @Parameter(defaultValue = "${region}", readonly = true, required = false)
  protected String region;

  @Parameter(defaultValue = "${artifactRegion}", readonly = true, required = false)
  protected String artifactRegion;

  @Parameter(
      name = "baseContainerImage",
      defaultValue = "gcr.io/dataflow-templates-base/java11-template-launcher-base:latest",
      required = false)
  protected String baseContainerImage;

  public TemplateStageMojo() {}

  public TemplateStageMojo(
      MavenProject project,
      MavenSession session,
      File outputDirectory,
      File outputClassesDirectory,
      File resourcesDirectory,
      File targetDirectory,
      String projectId,
      String templateName,
      String bucketName,
      String stagePrefix,
      String region,
      String artifactRegion,
      String baseContainerImage) {
    this.project = project;
    this.session = session;
    this.outputDirectory = outputDirectory;
    this.outputClassesDirectory = outputClassesDirectory;
    this.resourcesDirectory = resourcesDirectory;
    this.targetDirectory = targetDirectory;
    this.projectId = projectId;
    this.templateName = templateName;
    this.bucketName = bucketName;
    this.stagePrefix = stagePrefix;
    this.region = region;
    this.artifactRegion = artifactRegion;
    this.baseContainerImage = baseContainerImage;
  }

  public void execute() throws MojoExecutionException {

    try {
      URLClassLoader loader = buildClassloader();

      BuildPluginManager pluginManager =
          (BuildPluginManager) session.lookup("org.apache.maven.plugin.BuildPluginManager");

      LOG.info("Staging Templates...");

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);
      for (TemplateDefinitions definition : templateDefinitions) {

        ImageSpec imageSpec = definition.buildSpecModel(false);
        String currentTemplateName = imageSpec.getMetadata().getName();

        // Filter out the template if there was a specific one given
        if (templateName != null
            && !templateName.isEmpty()
            && !templateName.equals(currentTemplateName)) {
          LOG.info("Skipping template {}", currentTemplateName);
          continue;
        }

        LOG.info("Staging template {}...", currentTemplateName);

        if (definition.isClassic()) {
          stageClassicTemplate(definition, imageSpec, pluginManager);
        } else {
          stageFlexTemplate(definition, imageSpec, pluginManager);
        }
      }

    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (MalformedURLException e) {
      throw new MojoExecutionException("URL generation failed", e);
    } catch (Exception e) {
      throw new MojoExecutionException("Template staging failed", e);
    }
  }

  /**
   * Stage a classic template. The way that it works, in high level, is:
   *
   * <p>Step 1: Use the Specs generator to create the metadata files.
   *
   * <p>Step 2: Use the exec:java plugin to execute the Template class, and passing parameters such
   * that the DataflowRunner knows that it is a Template staging (--stagingLocation,
   * --templateLocation). The class itself is responsible for copying all the dependencies to Cloud
   * Storage and saving the Template file.
   *
   * <p>Step 3: Use `gcloud storage` to copy the metadata / parameters file into Cloud Storage as
   * well, using `{templatePath}_metadata`, so that parameters can be shown in the UI when using
   * this specific Template.
   */
  protected String stageClassicTemplate(
      TemplateDefinitions definition, ImageSpec imageSpec, BuildPluginManager pluginManager)
      throws MojoExecutionException, IOException, InterruptedException {
    File metadataFile =
        new TemplateSpecsGenerator()
            .saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory);
    String currentTemplateName = imageSpec.getMetadata().getName();

    String stagingPath = "gs://" + bucketName + "/" + stagePrefix + "/staging/";
    String templatePath = "gs://" + bucketName + "/" + stagePrefix + "/" + currentTemplateName;
    String templateMetadataPath = templatePath + "_metadata";

    executeMojo(
        plugin("org.codehaus.mojo", "exec-maven-plugin"),
        goal("java"),
        configuration(
            // Base image to use
            element("mainClass", imageSpec.getMetadata().getMainClass()),
            element("cleanupDaemonThreads", "false"),
            element(
                "arguments",
                element("argument", "--runner=DataflowRunner"),
                element("argument", "--stagingLocation=" + stagingPath),
                element("argument", "--templateLocation=" + templatePath),
                element("argument", "--project=" + projectId),
                element("argument", "--region=" + region))),
        executionEnvironment(project, session, pluginManager));

    String[] copyCmd =
        new String[] {
          "gcloud", "storage", "cp", metadataFile.getAbsolutePath(), templateMetadataPath
        };
    LOG.info("Running: {}", String.join(" ", copyCmd));

    Process process = Runtime.getRuntime().exec(copyCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error copying template using 'gcloud storage'. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }

    LOG.info("Classic Template was staged! {}", templatePath);
    return templatePath;
  }

  /**
   * Stage a Flex template. The way that it works, in high level, is:
   *
   * <p>Step 1: Use the Specs generator to create the metadata and command spec files.
   *
   * <p>Step 2: Use JIB to create a Docker image containing the Template code, and upload to
   * Artifact Registry / GCR.
   *
   * <p>Step 3: Use `gcloud dataflow build` to create the Template based on the image created by
   * JIB, and uploads the metadata file to the proper directory.
   */
  protected String stageFlexTemplate(
      TemplateDefinitions definition, ImageSpec imageSpec, BuildPluginManager pluginManager)
      throws MojoExecutionException, IOException, InterruptedException {

    String currentTemplateName = imageSpec.getMetadata().getName();
    TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

    String prefix = "";
    if (artifactRegion != null && !artifactRegion.isEmpty()) {
      prefix = artifactRegion + ".";
    }

    String containerName = definition.getTemplateAnnotation().flexContainerName();
    String imagePath =
        prefix + "gcr.io/" + projectId + "/" + stagePrefix.toLowerCase() + "/" + containerName;
    LOG.info("Stage image to GCR: {}", imagePath);

    File metadataFile =
        generator.saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory);
    File commandSpecFile = generator.saveCommandSpec(definition, outputClassesDirectory);

    String appRoot = "/template/" + containerName;
    String commandSpec = appRoot + "/resources/" + commandSpecFile.getName();

    executeMojo(
        plugin("com.google.cloud.tools", "jib-maven-plugin"),
        goal("build"),
        configuration(
            // Base image to use
            element("from", element("image", baseContainerImage)),
            // Target image to stage
            element("to", element("image", imagePath)),
            element(
                "container",
                element("appRoot", appRoot),
                // Keep the original entrypoint
                element("entrypoint", "INHERIT"),
                // Point to the command spec
                element("environment", element("DATAFLOW_JAVA_COMMAND_SPEC", commandSpec)))),
        executionEnvironment(project, session, pluginManager));

    String templatePath = "gs://" + bucketName + "/" + stagePrefix + "/flex/" + currentTemplateName;
    String[] flexTemplateBuildCmd =
        new String[] {
          "gcloud",
          "dataflow",
          "flex-template",
          "build",
          templatePath,
          "--image",
          imagePath,
          "--project",
          projectId,
          "--sdk-language=JAVA",
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile.getName(),
        };
    LOG.info("Running: {}", String.join(" ", flexTemplateBuildCmd));

    Process process = Runtime.getRuntime().exec(flexTemplateBuildCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error building template using gcloud. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }

    LOG.info("Flex Template was staged! {}", templatePath);
    return templatePath;
  }
}
