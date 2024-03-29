#' run test
#'
#' @export
run_test2 <- function(dbf,cohort=NULL,symptomtab=NULL,cortype='bon',ratioval=1,pcut=0.05,
                     groupat="medcodeid",db=F,exportfile=NULL){
  if(F){
    dbf <- dbfile
    cohort <- 'behcets'
    symptomtab <- "symptomcodes"
    cortype <- 'bon'
    ratioval <- 1
    pcut <- 0.05
    groupat <- "medcodeid"
    db=T
  }

  strsql <- str_c(
  "SELECT DISTINCT
    h.study,
    h.cohort,
    h.",groupat,",
    COUNT(h.patid) AS occur
  FROM(
    SELECT DISTINCT
      c.study,
      c.cohort,
      s.",groupat,",
      p.patid
    FROM 
      studycohorts AS c
    INNER JOIN
      fullindexages AS a
      ON a.patid=c.patid AND c.study=a.codetype AND c.cohort=a.cohort
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
      a.codetype='",cohort,"' AND
      YEAR(CAST(o.obsdate AS DATE)) - p.yob <= a.iage
    GROUP BY
      p.patid,
      c.study,
      c.cohort,
      s.",groupat,") AS h
  GROUP BY
    h.cohort,
    h.study,
    h.",groupat,";
  "
  )


  if(db) writeLines(strsql)
  ddb <- duckdb::dbConnect(duckdb(),dbfile,read_only=T)

  obsfreq <- dbGetQuery(ddb,strsql) %>% tibble() %>% 
    pivot_wider(names_from='cohort',values_from='occur',values_fill=0)
  duckdb::dbDisconnect(ddb,shutdown=T)
  if(db) obsfreq %>% display_data(disp=T)  
  strsql <- str_c("
  SELECT
    study,
    cohort,
    COUNT(patid) AS tots
  FROM 
    studycohorts
  WHERE 
    study LIKE '",cohort,"'
  GROUP BY
    study,
    cohort
  ")

  ddb <- duckdb::dbConnect(duckdb(),dbfile,read_only=T)
  cohtots <- dbGetQuery(ddb,strsql) %>% tibble() %>%
    pivot_wider(names_from='cohort',values_from='tots',values_fill=0)
  duckdb::dbDisconnect(ddb,shutdown=T)
  names(cohtots) <- paste0(names(cohtots),"_total")

  analset <- obsfreq %>% inner_join(cohtots,by=c("study"="study_total")) 

  resset <- apply(analset,1,test2_medcode) %>% lapply(unlist) %>% plyr::ldply() %>%
   mutate(ratio=signif(as.numeric(ratio),3)) 
  resset$padj <- round(p.adjust(resset$pvalue, cortype, n=nrow(resset)),5)
  sigres <- resset %>% dplyr::filter(ratio>=ratioval & padj<=pcut) %>% arrange(padj)
  if(!is.null(exportfile)){
    sigres %>% write_csv(file=exportfile)
  }
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
  if(db) result %>% display_data(disp=T)
  return(result)
}

#' @export
test2_medcode <- function(mcl,groupat){
  mc <- mcl[groupat]
  cases <- as.integer(mcl["cases"])
  conts <- as.integer(mcl["conts"])
  casetotal <- as.integer(mcl["cases_total"])
  conttotal <- as.integer(mcl["conts_total"])
  casep <- cases/casetotal
  contp <- conts/conttotal
  tv <- DescTools::GTest(as.data.frame(matrix(c(cases,conts,casetotal,conttotal),nrow=2)))
  list(mc,pvalue=unname(tv$p.value),caseprop=casep,contprop=contp,ratio=casep/contp)
}
