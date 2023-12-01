#' run test
#'
#' @export
run_test <- function(dbf,cohort=NULL,symptomtab=NULL,cortype='bon',ratioval=1,pcut=0.05,
                     groupat="medcodeid"){
  if(F){
    dbf <- dbfile
    cohort <- 'sle'
    symptomtab <- "symptomcodes"
    cortype <- 'bon'
    ratioval <- 1
    pcut <- 0.05
    groupat <- "snomedctconceptid"
  }
  strsql <- str_c("
  SELECT DISTINCT
    c.cohort,
    c.status,
    s.",groupat,",
    COUNT(o.obsdate) AS occur
  FROM 
    cohorts AS c
  INNER JOIN 
    patients AS p
    ON p.patid=c.patid
  INNER JOIN
    observations AS o
    ON p.patid=o.patid
  INNER JOIN
    ",symptomtab," AS s
    ON s.medcodeid=o.medcodeid
  WHERE
    c.cohort='",cohort,"' AND
    YEAR(CAST(o.obsdate AS DATE)) - p.yob <= c.indexage
  GROUP BY
    c.cohort,
    c.status,
    s.",groupat,";
  ")

  ddb <- duckdb::dbConnect(duckdb(),dbfile,read_only=T)
  obsfreq <- dbGetQuery(ddb,strsql) %>% tibble() %>% 
    pivot_wider(names_from='status',values_from='occur',values_fill=0)
  duckdb::dbDisconnect(ddb,shutdown=T)

  strsql <- str_c("
  SELECT
    cohort,
    status,
    COUNT(patid) AS tots
  FROM 
    cohorts
  WHERE 
    cohort LIKE '",cohort,"'
  GROUP BY
    cohort,
    status
  ")

  ddb <- duckdb::dbConnect(duckdb(),dbfile,read_only=T)
  cohtots <- dbGetQuery(ddb,strsql) %>% tibble() %>%
    pivot_wider(names_from='status',values_from='tots',values_fill=0)
  duckdb::dbDisconnect(ddb,shutdown=T)
  names(cohtots) <- paste0(names(cohtots),"_total")

  analset <- obsfreq %>% inner_join(cohtots,by=c("cohort"="cohort_total"))
  resset <- apply(analset,1,test_medcode) %>% lapply(unlist) %>% plyr::ldply() %>%
   mutate(ratio=as.numeric(ratio)) 
  resset$padj <- p.adjust(resset$pvalue, cortype, n=nrow(resset))
  sigres <- resset %>% dplyr::filter(ratio>=ratioval & padj<=pcut) %>% arrange(padj)
  
  strsql <- str_c("
    SELECT DISTINCT
      medcodeid,
      term,
      snomedctconceptid 
    FROM
      '",symptomtab,"'")

  ddb <- duckdb::dbConnect(duckdb(),dbfile,read_only=T)
  anotation <- dbGetQuery(ddb,strsql) %>% tibble()
  duckdb::dbDisconnect(ddb,shutdown=T)

  result <- anotation %>% inner_join(sigres,by=groupat)
  return(result)
}

#' @export
test_medcode <- function(mcl,groupat){
  mc <- mcl[groupat]
  cases <- as.integer(mcl["case"])
  conts <- as.integer(mcl["cont"])
  casetotal <- as.integer(mcl["case_total"])
  conttotal <- as.integer(mcl["case_total"])
  casep <- cases/casetotal
  contp <- conts/conttotal
  tv <- DescTools::GTest(as.data.frame(matrix(c(cases,conts,casetotal,conttotal),nrow=2)))
  list(mc,pvalue=unname(tv$p.value),cases=cases,controls=conts,ratio=casep/contp)
}
