#' get dmplusd codes by vtm
#'
#' @export
get_dmd_vtm <- function(vtmtext,dbFile,db=F){
  if(db){
    vtmtext <- "%valpr%"
    dbFile <- dbName
  }

  flat_sql <- str_c("
    SELECT DISTINCT
      vmp.VPID,
      vmp.NM AS vmpname,
      amp.APID, 
      amp.NM AS ampname,
      vmpp.VPPID, 
      vmpp.NM AS vmppname,
      ampp.APPID, 
      ampp.NM AS amppname,
      moi.moiety 
    FROM 
      vmp AS vmp
    INNER JOIN(
      SELECT DISTINCT
        vtm.NM AS moiety,
        vtm.VTMID AS vtmid
      FROM 
        vtm AS vtm
      WHERE 
        LOWER(vtm.NM) LIKE '",tolower(vtmtext),"') AS moi
      ON vmp.VTMID = moi.vtmid
    INNER JOIN 
      amp AS amp
      ON amp.VPID=vmp.VPID
    INNER JOIN 
      vmpp AS vmpp
      ON vmpp.VPID=vmp.VPID
    INNER JOIN 
      ampp AS ampp
      ON ampp.APID=amp.APID
  ")

  dbc <- dbConnect(duckdb(),dbFile)
  vtms <- dbGetQuery(dbc,flat_sql) %>% tibble() %>% 
    pivot_longer(c(VPID,APID,VPPID,APPID),names_to="dmdtype",values_to="dmdcode")
  dbDisconnect(dbc)
  return(vtms)
}
