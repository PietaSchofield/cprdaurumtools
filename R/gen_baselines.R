#' generate base line table
#'
#'
#' @export
gen_baselines <- function(dbf,tabnew="covariate_observations",tabname){
  if(F){
    tabname <- "baseline_codes"
    tabnew <- "covariate_observations"
    dbf <- dbif
  }

  blo_sql <- str_c("
    CREATE TABLE 
      ",tabnew," AS
    SELECT DISTINCT
      o.patid,
      o.medcodeid,
      o.obsdate,
      o.value,
      o.numrangelow AS minval,
      o.numrangehigh AS maxval,
      nu.description AS units,
      bl.cv AS baseline
    FROM 
      observations AS o
    INNER JOIN
      ",tabname," AS bl
      ON bl.medcodeid=o.medcodeid
    LEFT JOIN
      aurum_numunit AS nu
      ON nu.numunitid=o.numunitid")

  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  if(!tabnew %in% dbListTables(dbi)){
    ext <- "new records"
    val <- dbExecute(dbi,blo_sql)
  }else{
    ext <- "records exist"
    val <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",tabnew))
  }
  dbDisconnect(dbi,shutdown=T)
  return(paste0(val," ",ext))
}
