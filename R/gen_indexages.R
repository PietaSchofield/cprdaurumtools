#' get index date for cases
#'
#'
#' @import magrittr
#' @export
gen_indexdist <- function(dbf,idxtab="fullindexages",pats="patients",ctype="%",seed=NULL){
  if(F){
    dbf=dbfile
    ctype="%"
    idxtab="fullindexages"
    pats="patients"
    seed=NULL
  }
  set.seed(seed)
  indexages <- get_cohort(dbf,indexfield="indexage",vfill=NA,codetype=ctype) %>%
    pivot_longer(-c(patid,cohort),names_to="codetype",values_to="age")
  patients <- get_patients(dbf,fields=c("patid"))
  cts <- indexages %>% pull(codetype) %>% unique()
  names(cts) <- cts
  ct <- cts[1]
  res <- lapply(cts,function(ct){
    dat <- indexages %>% dplyr::filter(codetype==ct & !is.na(age)) %>% pull(age) 
    caseages <- indexages %>% dplyr::filter(codetype==ct & !is.na(age))
    ret <- patients %>% left_join(caseages,by=c("patid")) %>%
      mutate(cohort=ifelse(is.na(cohort),"conts",cohort),
             codetype=ct)
    ret$page <- sample(dat,size=nrow(ret),replace=T) 
    ret %>% mutate(iage=ifelse(is.na(age),page,age)) %>% select(patid,cohort,codetype,iage) %>% tibble()
  }) %>% bind_rows()
  ddb <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  dbWriteTable(ddb,idxtab,res,overwrite=T)
  dbDisconnect(ddb,shutdown=T)
  nrow(res)
}
