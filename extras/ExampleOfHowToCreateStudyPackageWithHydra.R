remotes::install_github("OHDSI/Hydra")
outputFolder <- "d:/temp/output"  # location where you study package will be created


########## Please populate the information below #####################
version <- "v0.1.0"
name <- "stemiMyocardialInfarction - an OHDSI network study"
packageName <- "stemiMyocardialInfarction"
skeletonVersion <- "v0.0.1"
createdBy <- "rao@ohdsi.org"
createdDate <- Sys.Date() # default
modifiedBy <- "rao@ohdsi.org"
modifiedDate <- Sys.Date()
skeletonType <- "CohortDiagnosticsStudy"
organizationName <- "OHDSI"
description <- "Cohort diagnostics on stemiMyocardialInfarction cohorts."

stemiCohorts <- dplyr::tibble(
  atlasDemoCohortId = c(1781982,
                        1781983,
                        1781985,
                        1781986,
                        1781987,
                        1781984),
  iqviaCohortId = c(10444, 10445, 10446, 10447, 10448, 1781984)
)


library(magrittr)
# Set up# from atlas-phenotype.ohdsi.org (note: approved phenotypes do not have '[')
baseUrl <- "https://atlas-demo.ohdsi.org/WebAPI"
webApiCohorts <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl)
studyCohorts <-  webApiCohorts %>%
  dplyr::rename(atlasDemoCohortId = id) |> 
  dplyr::inner_join(stemiCohorts) |> 
  dplyr::rename(id = iqviaCohortId) |> 
  dplyr::relocate(id)

studyCohorts <- dplyr::bind_rows(studyCohorts |> 
                                   dplyr::mutate(source = 'atlas'),
                                 PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = c(142, 260)) |> 
                                   dplyr::mutate(source = 'pl')
                                 )

# compile them into a data table
cohortDefinitionsArray <- list()
for (i in (1:nrow(studyCohorts))) {
  studyCohort <- studyCohorts[i,]
  
  if (studyCohort$source == 'atlas') {
    cohortDefinition <-
      ROhdsiWebApi::getCohortDefinition(cohortId = studyCohorts$atlasDemoCohortId[[i]],
                                        baseUrl = baseUrl)
    cohortDefinitionsArray[[i]] <- list(
      id = studyCohorts$id[[i]],
      createdDate = studyCohorts$createdDate[[i]],
      modifiedDate = studyCohorts$createdDate[[i]],
      logicDescription = studyCohorts$description[[i]],
      name = stringr::str_trim(stringr::str_squish(cohortDefinition$name)),
      expression = cohortDefinition$expression
    )
  } else {
    cohortDefinition <- studyCohort$json |> RJSONIO::fromJSON(digits = 23)
    cohortDefinitionsArray[[i]] <- list(
      id = studyCohort$cohortId,
      createdDate = Sys.Date(),
      modifiedDate = Sys.Date(),
      logicDescription = studyCohort$cohortName,
      name = studyCohort$cohortName,
      expression = cohortDefinition
    )
  }
}

tempFolder <- tempdir()
unlink(x = tempFolder, recursive = TRUE, force = TRUE)
dir.create(path = tempFolder, showWarnings = FALSE, recursive = TRUE)

specifications <- list(id = 1,
                       version = version,
                       name = name,
                       packageName = packageName,
                       skeletonVersion = skeletonVersion,
                       createdBy = createdBy,
                       createdDate = createdDate,
                       modifiedBy = modifiedBy,
                       modifiedDate = modifiedDate,
                       skeletonType = skeletonType,
                       organizationName = organizationName,
                       description = description,
                       cohortDefinitions = cohortDefinitionsArray)

jsonFileName <- paste0(file.path(tempFolder, "CohortDiagnosticsSpecs.json"))
write(x = specifications %>% RJSONIO::toJSON(pretty = TRUE, digits = 23), file = jsonFileName)


##############################################################
##############################################################
#######       Get skeleton from github            ############
#######       Uncomment if you want to use latest ############
#######       skeleton only - for advanced user   ############
##############################################################
##############################################################
##############################################################
#### get the skeleton from github
download.file(url = "https://github.com/OHDSI/SkeletonCohortDiagnosticsStudy/archive/refs/heads/main.zip",
                         destfile = file.path(tempFolder, 'skeleton.zip'))
unzip(zipfile =  file.path(tempFolder, 'skeleton.zip'),
      overwrite = TRUE,
      exdir = file.path(tempFolder, "skeleton")
        )
fileList <- list.files(path = file.path(tempFolder, "skeleton"), full.names = TRUE, recursive = TRUE, all.files = TRUE)
DatabaseConnector::createZipFile(zipFile = file.path(tempFolder, 'skeleton.zip'),
                                 files = fileList,
                                 rootFolder = list.dirs(file.path(tempFolder, 'skeleton'), recursive = FALSE))

##############################################################
##############################################################
#######               Build package              #############
##############################################################
##############################################################
##############################################################


#### Code that uses the ExampleCohortDiagnosticsSpecs in Hydra to build package
hydraSpecificationFromFile <- Hydra::loadSpecifications(fileName = jsonFileName)
unlink(x = outputFolder, recursive = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

Hydra::hydrate(specifications = hydraSpecificationFromFile,
               outputFolder = outputFolder,
               skeletonFileName = file.path(tempFolder, 'skeleton.zip')
)


unlink(x = tempFolder, recursive = TRUE, force = TRUE)


##############################################################
##############################################################
######       Build, install and execute package           #############
##############################################################
##############################################################
##############################################################
