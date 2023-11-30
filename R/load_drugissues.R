# get_drugissues_batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#' @param bpp BiocParallal Multicore Parameters
#'
#' @export
load_drugissues <- function(pddir,dbf,ow=F,db=F,tab_name="drug_issues",
    selvars2=c("patid","prodcodeid","issuedate","dosageid","quantity","quantunitid","duration")){
  if(F){
    pddir <- datadir
    dbf <- dbfile
    ow <- F
    db <- F
    tab_name <- "drug_issues"
    selvars2 <- c("patid","prodcodeid","issuedate","dosageid","quantity","quantunitid","duration")
  }
  difiles <- list.files(pddir,pattern="Drug",full=T,recur=T)
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- dbListTables(dbi)
  duckdb::dbDisconnect(dbi)  
  nrec <- 0
  if(!tab_name%in%tabs || ow){
    if(tab_name%in%tabs){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
      duckdb::dbDisconnect(dbi)  
    }
    nrec <- lapply(difiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars2)) %>%
        dplyr::mutate(issuedate = format(lubridate::dmy(issuedate))) 
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
      duckdb::dbDisconnect(dbi)  
      nr <- dat %>% nrow()
      cat(paste0(fn," ",nr," records loaded\n"))
      rm(dat)
      gc()
    })
  }
  trec <- nrec %>% unlist() %>% sum()
  return(paste0(trec," records loaded"))
}
