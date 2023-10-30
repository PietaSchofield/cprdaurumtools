#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
proc_patients <- function(dbpost=dbpost,dbpre=dbpre,ow=F,db=F,
  newtab="patients",oldtab="patients",
  jointabs=list(opats="death_patient",hpats="hes_patient",ipats="patient_2019_imd")){
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbpre)
  dbo <- duckdb::dbConnect(duckdb::duckdb(),dbpost)
  ret <- 0
  if(!newtab%in%duckdb::dbListTables(dbo)||ow){
    afields <- paste0(oldtab,".",duckdb::dbListFields(dbi,oldtab),collapse=" , ")
    jfields <- paste0(unlist(sapply(jointabs,function(jt) paste0(jt,".",
                        setdiff(duckdb::dbListFields(dbi,jt),duckdb::dbListFields(dbi,oldtab))))),
                 collapse=" , ")
    jjoin <- paste0(sapply(jointabs,function(jt)
        paste0("LEFT JOIN ",jt," ON ", 
        paste0(sapply(intersect(duckdb::dbListFields(dbi,jt),duckdb::dbListFields(dbi,oldtab)),
          function(jf)paste(c(jt,oldtab),jf,sep=".",collapse="=")),collapse=" AND "))),collapse=" ")
    str_sql <- paste("SELECT",afields,",",jfields,"FROM",oldtab,jjoin,collapse=" ")
    patdata <- duckdb::dbGetQuery(dbi,str_sql)
    ret <- patdata %>% nrow()
    duckdb::dbWriteTable(dbo,"patients",patdata,overwrite=T)
    rm(patdata)
    gc()
  }
  duckdb::dbDisconnect(dbi)
  duckdb::dbDisconnect(dbo)
  return(paste(ret,"records processed"))
}
