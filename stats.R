library('rjson')
library('tidyverse')

# Sub-folders
sub.folders <- list.dirs(paste0(getwd(), '/output'), recursive = T)[-1]

# Declaring an empty list (len = nb sub-folders)
file_list <- rep(list(NA), length(sub.folders))

# Md file
sink("stats.md")

cat('# Basic data-related statistics\n')
cat('## Number of missions\n')

for (i in 1:length(file_list)) {
  
  file_list[[i]] <- dir(sub.folders[i]) %>% str_sort(numeric = T)
  sum_res <- 0
  
  year <- str_extract(sub.folders[i], "([2][0][0-9][0-9])")
  
  cat('###', year, '\n')
  cat('| Period |  Value   |\n')
  cat('| ------ | :------: |\n')
  
  for (j in 1:length(file_list[[i]])) {
    result <- fromJSON(file = paste0(sub.folders[i], '/', file_list[[i]][j]))$missions %>% length()
    
    month <- str_extract(
      file_list[[i]][j] %>% 
        tools::file_path_sans_ext(), 
      "[^_]+$"
    ) %>% stringr::str_to_sentence()
    
    cat("|", month, "|", result, "|\n")
    
    sum_res <- sum_res + result
  }
  
  cat("| TOTAL", year, "|", sum_res, "|\n\n")
}

sink()