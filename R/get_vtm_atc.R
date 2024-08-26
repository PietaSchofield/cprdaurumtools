#' get dmplusd codes by vtm
#'
#' @export
get_vtm_atc <- function(dbFile,db=F){
  if(db){
    dbFile <- dmddb
  }

  flat_sql <-  str_c("
    SELECT DISTINCT
      vtm.NM AS moiety, 
      fat.ATC AS atc_code,
    FROM 
      vmp AS vmp
    INNER JOIN
      vtm AS vtm
      ON vmp.VTMID = vtm.vtmid
    INNER JOIN 
      bnfatc AS fat
      ON vmp.VPID=fat.VPID
    WHERE
      fat.ATC IS NOT NULL AND
      fat.ATC NOT LIKE 'n/a' AND
      vtm.NM NOT LIKE '% + %'
  ")

  dbc <- dbConnect(duckdb(),dbFile)
  vtms <- dbGetQuery(dbc,flat_sql) %>% tibble() 
  dbDisconnect(dbc)

  return(vtms)
}
