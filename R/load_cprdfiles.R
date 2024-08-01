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
  if(F){
    pddir <- rdir
    dbf <- dbif
    ow=F
    db=F
  }
  cprdfiles <- list.files(pddir,pattern=".*txt",full=T)
  names(cprdfiles) <- paste0("aurum_",tolower(gsub("(^[0-9]*_|[.]txt)","",basename(cprdfiles))))
  lapply(names(cprdfiles),function(fn){
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    if(!fn%in%dbListTables(dbi) || ow){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      dat <- readr::read_tsv(cprdfiles[[fn]],
                             col_types=readr::cols(.default=readr::col_character()),
                             locale=locale(encoding="ISO-8859-1")) %>%
        as_tibble()
      names(dat) <- tolower(names(dat))
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      duckdb::dbDisconnect(dbi,shutdown=T)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
    }else{
      nr <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",fn))
      duckdb::dbDisconnect(dbi,shutdown=T)
      ext <- "records exist"
    }
    cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
  })
  return()
}
