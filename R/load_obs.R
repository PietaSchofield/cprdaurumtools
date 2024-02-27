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
load_obs <- function(pddir,dbf,ow=F,db=F,tab_name="observations",
    selvars=c("patid","consid","parentobsid","probobsid","medcodeid","obsdate","value","numunitid",
              "numrangelow","numrangehigh")){
  if(F){
    pddir <- datadir
    dbf <- dbfile
    ow <- T
    db <- F
    tab_name <- "observations"
    selvars <- c("patid","consid","parentobsid","probobsid","medcodeid","obsdate","value","numunitid",
                 "numrangelow","numrangehigh")
  }
  obsfiles <- list.files(pddir,pattern="Obs.*txt$",full=T,recur=T)
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- dbListTables(dbi)
  duckdb::dbDisconnect(dbi)
  nrec <- 0
  if(!tab_name%in%tabs || ow){
    if(tab_name%in%tabs){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
      duckdb::dbDisconnect(dbi)
    }
    nrec <- lapply(obsfiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars)) %>%
        dplyr::mutate(obsdate = format(lubridate::dmy(obsdate)))
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
      duckdb::dbDisconnect(dbi)
      nr <- dat %>% nrow()
      cat(paste0(basename(fn),": ",nr," records loaded\n"))
      rm(dat)
      gc()
      return(nr)
    })
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
