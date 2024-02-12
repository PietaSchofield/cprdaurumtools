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
load_imdfiles <- function(pddir,dbf,ow=T,db=F,tad,pats){
  if(db){
    pddir <- idir
    tad <- "21_001631"
    dbf <- "/home/pietas/Projects/t2dd/.data/t2dd_prep.duckdb" 
    ow <- T
    pats <- patids
  }
  imdfiles <- list.files(pddir,pattern="_imd_.*txt",full=T)
  names(imdfiles) <- tolower(gsub(paste0("_",tad,".*[.]txt"),"",basename(imdfiles)))
  lapply(names(imdfiles),function(fn){
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    if(!fn%in%duckdb::dbListTables(dbi) || ow){
      dat <- readr::read_tsv(imdfiles[[fn]],col_types=readr::cols(.default=readr::col_character())) %>%
        as_tibble() %>% dplyr::filter(patid%in%pats)
      names(dat) <- tolower(names(dat))
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      nr <- dat %>% nrow()
      cat(paste0(basename(fn),": ",nr," records loaded\n"))
      duckdb::dbDisconnect(dbi)
      rm(dat)
      gc()
    }else{
      duckdb::dbDisconnect(dbi)
    }
  })
  return()
}
