#' get observations batch
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
load_practice <- function(pddir,dbf,ow=F,db=F,tab_name="practices",
    selvars=c("pracid","lcd","uts","region")){
  if(F){
    pddir <- datadir
    dbf <- dbfile
    ow <- F 
    db <- F
    tab_name <- "practices"
    selvars <- c("pracid","lcd","uts","region")
  }
  obsfiles <- list.files(pddir,pattern="Prac.*txt$",full=T,recur=T)
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
      readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars)) %>%
        dplyr::mutate(lcd = format(lubridate::dmy(lcd)),
                      uts = format(lubridate::dmy(uts)))
      }) %>% bind_rows() %>% unique()
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
    duckdb::dbDisconnect(dbi,shutdown=T)
    nrec <- dat %>% nrow()
    rm(dat)
    gc()
    ext <- "new records"
  }else{
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    nrec <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    dbDisconnect(dbi)
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
