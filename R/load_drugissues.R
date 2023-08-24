# get_drugissues_batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param batch the name of the batch subdirectory
#' @param datadir the name of the data directory
#' @param bpp BiocParallal Multicore Parameters
#'
#' @export
get_di_batch_summary <- function(bdir,odir,dl=NULL,bpp=BiocParallel::bpparam(),ow=F,db=F){
  difiles <- list.files(pddir,pattern="Drug",full=T)
  if(!file.exists(dirdsfile) | ow){
    selvars2 <- c("patid","prodcodeid","issuedate")
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        {if(!is.null(dl)) filter(.,prodcodeid%in%dl) else . } %>%
        dplyr::select(dplyr::all_of(selvars2)) %>%
        dplyr::mutate(issuedate = lubridate::dmy(issuedate)) %>%
        dplyr::group_by(.data$patid,.data$prodcodeid) %>%
        dplyr::summarise(issues=dplyr::n(),
                         firstissue=min(issuedate),
                         lastissue=max(issuedate),.groups="drop")
    }
    ,BPPARAM=bpp)) %>% 
    dplyr::group_by(.data$patid,.data$prodcodeid) %>%
    dplyr::summarise(issues=sum(issues),
                     firstissue=min(firstissue),
                     lastissue=max(lastissue),.groups="drop") %>% 
    saveRDS(file=dirdsfile)
    if(db) dta <- readRDS(dirdsfile)
  }
  return(dirdsfile)
}
