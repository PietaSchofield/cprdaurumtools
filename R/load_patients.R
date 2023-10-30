#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
load_patients <- function(pdir,dbf,ow=F,db=F,tabname="patients",
   selvars1=c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")){
  patfiles <- list.files(pdir,pattern="Patient",full=T,recur=T)
  names(patfiles) <- gsub(paste0("(^",pdir,"/|/Patient/.*)"),"",patfiles)
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  ret <- 0
  if(!tabname%in%duckdb::dbListTables(dbi) || ow){
    dat <- plyr::ldply(lapply(patfiles,function(fn){
       readr::read_tsv(fn,col_type=readr::cols(.default=readr::col_character())) %>%
                     dplyr::select(all_of(selvars1)) %>%
                     dplyr::mutate(yob = as.integer(yob),
                            emis_ddate = format(lubridate::dmy(emis_ddate)),
                            regstartdate = format(lubridate::dmy(regstartdate)),
                            regenddate = format(lubridate::dmy(regenddate)),
                            cprd_ddate = format(lubridate::dmy(cprd_ddate)))
                     }))
    ret <- dat %>% nrow()
    duckdb::dbWriteTable(dbi,tabname,dat,overwrite=T,append=F)
    rm(dat)
    gc()
  }
  duckdb::dbDisconnect(dbi,shutdown=T)
  return(cat(paste0(ret," records processed\n")))
}
