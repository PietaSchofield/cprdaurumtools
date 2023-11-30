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
#' @import magrittr
#' @export
load_staff <- function(pddir,dbf,ow=F,db=F,tab_name="staff"){
  if(F){
    pddir <- datadir
    dbf <- dbfile
    ow <- F
    db <- F
    tab_name <- "staff"
  }
  obsfiles <- list.files(pddir,pattern="Staff.*txt$",full=T,recur=T)
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- dbListTables(dbi)
  dbDisconnect(dbi)
  nrec <- 0
  if(!tab_name%in% tabs || ow){
    if(tab_name%in% tabs){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
      dbDisconnect(dbi)
    }
    dat <- lapply(obsfiles,function(fn){
             dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) 
           }) %>% bind_rows() %>% unique() 
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
    dbDisconnect(dbi)
    nred <- dat %>% nrow()
    rm(dat)
    gc()
  }
  return(cat(paste0(nrec," records processed\n")))
}
