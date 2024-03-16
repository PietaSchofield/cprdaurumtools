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
load_onsfiles <- function(pddir,dbf,ow=T,db=F,tad,pats){
  if(db){
    pddir <- odir
    tad <- "21_001631"
    dbf <- dbif
    ow <- T
    pats <- patids
  }  
  onsfiles <- list.files(pddir,pattern="death_.*txt",full=T)
  names(onsfiles) <- tolower(gsub(paste0("_",tad,".*[.]txt"),"",basename(onsfiles)))
  lapply(names(onsfiles),function(fn){
    fn <- names(onsfiles)[1]
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    if(!fn%in%duckdb::dbListTables(dbi) || ow){
      dat <- readr::read_tsv(onsfiles[[fn]],col_types=readr::cols(.default=readr::col_character())) %>%
        as_tibble() %>% dplyr::filter(patid%in%pats) %>%
        dplyr::mutate(dor=lubridate::dmy(dor),
                      dod=lubridate::dmy(dod)) 
      names(dat) <- tolower(names(dat))
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      duckdb::dbDisconnect(dbi)
      ext <- "new records"
      nr <- dat %>% nrow()
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
