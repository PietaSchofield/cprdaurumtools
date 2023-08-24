#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
load_patients <- function(pddir,dbf,ow=F,db=F,tabname="patients",
   selvars1=c("patid","pracid","gender","yob","regstartdate","regenddate","emis_ddate","cprd_ddate")){
  patfiles <- list.files(pddir,pattern="Patient",full=T)
  dbi <- RSQLite::dbConnect(RSQLite::SQLite(),dbf)
  ret <- 0
  if(!tabname%in%RSQLite::dbListTables(dbi) || ow){
    dat <- dplyr::bind_rows(BiocParallel::bplapply(patfiles,function(fn){
       readr::read_tsv(fn,col_type=readr::cols(.default=readr::col_character())) %>%
                     dplyr::select(all_of(selvars1)) %>%
                     dplyr::mutate(yob = as.integer(yob),
                            emis_ddate = lubridate::dmy(emis_ddate),
                            regstartdate = lubridate::dmy(regstartdate),
                            regenddate = lubridate::dmy(regenddate),
                            cprd_ddate = lubridate::dmy(cprd_ddate))
                     }))
    ret <- dat %>% nrow()
    RSQLite::dbWriteTable(dbi,tabname,dat,overwrite=T,append=F)
    rm(dat)
    gc()
  }
  RSQLite::dbDisconnect(dbi)
  return(cat(paste0(ret," records processed\n")))
}
