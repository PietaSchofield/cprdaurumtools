#' Extract the mpi
#'
#'
#' @export
extract_mpi <- function(dbf,dbo,db=F,ow=F){
  if(db){
    dbf <- dbif
    dbo <- dbof
  }

  mpi_sql <- str_c(" 
    SELECT
      p.patid AS p_patid,
      p.yob AS yob,
      p.regstartdate AS regstart,
      p.regenddate AS regend,
      p.cprd_ddate AS ddcprd,
      p.emis_ddate AS ddemis,
      b.indexdate AS indexdate,
      b.basedate AS basedate,
      t.patid AS t_patid,
      t.substance AS substance,
      CASE WHEN h.patid IS NULL THEN 'N' ELSE 'Y' END AS hes_record
    FROM 
      patients AS p
    INNER JOIN
      baseline_date AS b
      ON b.patid=p.patid
    LEFT JOIN(
      SELECT DISTINCT
        patid,
        substance
      FROM 
        drug_exposures AS d
      GROUP BY
        patid,
        substance) AS t
      ON t.patid=p.patid
    LEFT JOIN
      hes_patient AS h
      ON p.patid=h.patid"
  )

  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  mpi <- dbGetQuery(dbi,mpi_sql) %>% tibble()
  dbDisconnect(dbi,shutdown=T)
  dbo <- duckdb::dbConnect(duckdb::duckdb(),dbo)
  dbWriteTable(dbo,"master_patient_index",mpi,overwrite=T)
  dbDisconnect(dbo,shutdown=T)
}

