#' make a duckdb 
#'
#' @export
make_dmplusd_db <- function(filePath,dbPath=dirname(filePath),db=F,ow=F){

  if(db){
    filePath <- file.path(Sys.getenv("HOME"),"Projects","refdata","dmplusd","downloads")
    dbPath <- dirname(filePath)
    ow <- T
  }

  ver <- gsub("(^f_vtm2_|[.]xml$)","", list.files(filePath,pattern="f_vtm")[1])
  dbName <- file.path(dbPath,paste0("dmplusd_",ver,".duckdb"))
  if(!file.exists(dbName)| ow){
    dbc <- dbConnect(duckdb(),dbName)
    tabs <- dbListTables(dbc)
    dbDisconnect(dbc,shutdown=T)
   
    dmdxmldir <- filePath
    dmplusd <- list(
      vtm=list(filename=list.files(dmdxmldir,pattern="f_vtm",full=T),entity="VTM"),
      bnfatc <- list(filename=list.files(dmdxmldir,pattern="f_bnf",full=T,recur=T),entity="VMP"),
      vmp=list(filename=list.files(dmdxmldir,pattern="f_vmp[0-9]",full=T),entity="VMP"),
      amp=list(filename=list.files(dmdxmldir,pattern="f_amp[0-9]",full=T),entity="AMP"),
      vmpp=list(filename=list.files(dmdxmldir,pattern="f_vmpp",full=T),entity="VMPP"),
      ampp=list(filename=list.files(dmdxmldir,pattern="f_ampp",full=T),entity="AMPP"),
      ingredients=list(filename=list.files(dmdxmldir,pattern="f_ingredient",full=T),entity="ING"),
      lookups=list(filename=list.files(dmdxmldir,pattern="f_lookup",full=T),entity="INFO"))

    dmplusdlist <- BiocParallel::bplapply(dmplusd,function(dmd){
        xmlconvert::xml_to_df(file=dmd$filename,records.tag=dmd$entity,check.datatype=F)
    })

    lapply(names(dmplusdlist),function(dn){
      dbc <- dbConnect(duckdb(),dbName,write=T)
      dbWriteTable(dbc,dn,dmplusdlist[[dn]],overwrite=T)
      dbDisconnect(dbc,shutdown=T)
    })
   
  }
  return(dbName)
}
