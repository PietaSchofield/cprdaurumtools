#' get medcodes 
#'
#' @export
get_medcodes <- function(dbf,fields='*',codelist=NULL, whereclause=NULL){
  if(F){
    dbf <- dbfile
    fields <- '*'
    whereclause <- NULL
  }

  strsql <- str_c("
    SELECT DISTINCT
    ",paste0(fields,collapse=", "),"
    FROM 
      emismedicaldictionary ")
  if(!is.null(whereclause)){
    strsql <- cat(strsql,"\n",whereclause)
  }
  dbi <- dbConnect(duckdb(),dbf)
  medcodes <- dbGetQuery(dbi,strsql) %>% tibble()
  dbDisconnect(dbi)
  if(!is.null(codelist)){
    medcodes <- medcodes %>% dplyr::filter(medcodeid %in% codelist)
  }
  return(medcodes)
}
      
