#' load patients
#'
#' get the patient records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#'
#' @export
proc_patients <- function(dbpost=dbpost,dbpre=dbpre){
  dbi <- RSQLite::dbConnect(RSQLite::SQLite(),dbpre)
  dbo <- RSQLite::dbConnect(RSQLite::SQLite(),dbpost)
  apats <- str_c("patients")
  opats <- "death_patient"
  hpats <- "hes_patient"
  ipats <- "patient_2019_imd"
  afields <- RSQLite::dbListFields(dbi,apats)
  ifields <- RSQLite::dbListFields(dbi,ipats) %>% setdiff(afields)
  ijoin <- RSQLite::dbListFields(dbi,ipats) %>% intersect(afields)
  hfields <- RSQLite::dbListFields(dbi,hpats) %>% setdiff(afields)
  hjoin <- RSQLite::dbListFields(dbi,hpats) %>% intersect(afields)
  ofields <- RSQLite::dbListFields(dbi,opats) %>% setdiff(afields)
  ojoin <- RSQLite::dbListFields(dbi,opats) %>% intersect(afields)
  str_sql <- str_c("
    SELECT ",paste0("p.",afields,collapse=","),","
            ,paste0("i.",ifields,collapse=","),","
            ,paste0("h.",hfields,collapse=","),","
            ,paste0("o.",ofields,collapse=",")," 
    FROM ",apats," AS p 
    LEFT JOIN ",ipats," AS i
    ON ", paste0(sapply(ijoin,function(fn) paste0(c("p.","i."),fn,collapse="=")),collapse=" AND "),"
    LEFT JOIN ",opats," AS o
    ON ", paste0(sapply(ojoin,function(fn) paste0(c("p.","o."),fn,collapse="=")),collapse=" AND "),"
    LEFT JOIN ",hpats," AS h
    ON ", paste0(sapply(hjoin,function(fn) paste0(c("p.","h."),fn,collapse="=")),collapse=" AND "))
  patdata <- RSQLite::dbGetQuery(dbi,str_sql)
  ret <- patdata %>% nrow()
  RSQLite::dbWriteTable(dbo,"patients",patdata,overwrite=T)
  rm(patdata)
  gc()
  return(paste(ret,"records processed"))
}
