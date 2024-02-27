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
load_hesfiles <- function(pddir,dbf,ow=T,db=F,tad,pats){
  if(db){
    pddir <- hdir 
    dbf <- dbif
    tad <- "21_001631.*"
    pats <- patids
    ow <- F
  }
  hesfiles <- list.files(pddir,pattern=".*txt",full=T)
  names(hesfiles) <- tolower(gsub(paste0("_",tad,"[.]txt"),"",basename(hesfiles)))

  lapply(names(hesfiles),function(fn){
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    if(!fn%in%duckdb::dbListTables(dbi) || ow){
      dat <- readr::read_tsv(hesfiles[[fn]],col_types=readr::cols(.default=readr::col_character())) %>%
        as_tibble() %>% dplyr::filter(patid%in%pats)
      names(dat) <- tolower(names(dat))
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      duckdb::dbDisconnect(dbi)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
    }else{
      nr <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",fn))
      ext <- "records exist"
      duckdb::dbDisconnect(dbi)
    }
    cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
  })
  return()
}
