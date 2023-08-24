# get observations batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param bdir the name of the batch subdirectory
#' @param odir the name of the output directory
#' @param olist the list of observation codes
#' @param bpp BiocParallal Multicore Parameters
#'
#'
#' Pass a table of covariate codes and generate covariates table
#'
#' @export
load_cprdfiles <- function(pddir,dbf,ow=T,db=F){
  cprdfiles <- list.files(pddir,pattern=".*txt",full=T)
  dbi <- RSQLite::dbConnect(RSQLite::SQLite(),dbf)
  names(cprdfiles) <- tolower(gsub("[.]txt","",basename(cprdfiles)))
  lapply(names(cprdfiles),function(fn){
    if(!fn%in%RSQLite::dbListTables(dbi) || ow){
      dat <- readr::read_tsv(cprdfiles[[fn]],col_types=readr::cols(.default=readr::col_character())) %>%
        as_tibble()
      names(dat) <- tolower(names(dat))
      RSQLite::dbWriteTable(dbi,fn,dat,overwrite=T)
      nr <- dat %>% nrow()
      cat(paste0(basename(fn),": ",nr," records loaded\n"))
      rm(dat)
      gc()
    }
  })
  RSQLite::dbDisconnect(dbi)
  return()
}
