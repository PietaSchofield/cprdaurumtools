#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
load_patients <- function(pdir,dbf,ow=F,db=F,tab_name="patients",add=F,
   selvars1=c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")){
  if(db){
    tab_name <- "patients"
    ow <- F
    dbf <- sadb  
    pdir <- apath
    add <- T
   selvars1 <- c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")
  }
  if(add & ow){
    stop("Error both overwrite and append set. \n")
    return()
  }
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- duckdb::dbListTables(dbi)
  duckdb::dbDisconnect(dbi)
  if(!tab_name%in%tabs || ow || add){
    patfiles <- list.files(pdir,pattern="Patient",full=T,recur=T)
    patfiles
    dat <- lapply(patfiles,function(fn){
       readr::read_tsv(fn,col_type=readr::cols(.default=readr::col_character())) %>%
                     dplyr::select(all_of(selvars1)) %>%
                     dplyr::mutate(yob = as.integer(yob),
                            emis_ddate = format(lubridate::dmy(emis_ddate)),
                            regstartdate = format(lubridate::dmy(regstartdate)),
                            regenddate = format(lubridate::dmy(regenddate)),
                            cprd_ddate = format(lubridate::dmy(cprd_ddate)))
                     }) %>% bind_rows()
    nrec <- dat %>% nrow()
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf,write=T)
    duckdb::dbWriteTable(dbi,tab_name,dat,overwrite=ow,append=add)
    dbDisconnect(dbi)
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
  cat(paste0(trec," ",ext,"\n"))
  return(nrec)
}
