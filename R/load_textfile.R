#' load a text file
#'
#'
#'
#' @export
load_textfile <- function(pddir,dbf,filename,tabname,ow=F,db=F,sep="\t"){
  if(db){
    db <- F
    ow <- F
    tabname <- "diagnosis_codes"
    filename <- "Aurum_diabetes_type_2_codes.txt"
    db <- dbif
    pddir <- file.path(projRoot,"t2dd","reference","txt")
    sep <- "\t"
  }
  dat <- readr::read_delim(file.path(pddir,filename),col_type=cols(.default=col_character()),delim=sep)
  names(dat) <- names(dat) %>% tolower()          
  dat %>% select(term_in_file) %>% unique() %>% as.data.frame()
  dat <- dat %>% dplyr::select(medcodeid,term,snomedctconceptid)
  dbc <- duckdb::dbConnect(duckdb::duckdb(),dbf,write=T)
  if(ow| !tabname %in% dbListTables(dbc)){
    dbWriteTable(dbc,tabname,dat,over=ow)
  }
  duckdb::dbDisconnect(dbc,shutdown=T)
  return()
}
