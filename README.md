# CP-sign in Spark


Master thesis in Bioinformatics, 2018

Uppsala University

## Abstract 

Molecular signature fingerprints together with Support Vector Machines and Cross Conformal Prediction were used to train binary classification models for four different unbalanced and highly unbalanced Quantitative High-Throughput Screening. The validity of the predictor models was evaluated using the miscalibration rate, while the efficiency of the predictors was measured with fuzzines, sumP, region size, excess and the proportion of correct singletons predicted. By doing a grid search on the molecular signature height start and height end, the cost parameter in linear Support Vector Machine, and the number of folds in Cross Conformal Prediction, it was confirmed that the combination of height start 0 or 1, with height end 2 or 3 is the most adequate to build the molecular signature fingerprints to be used as input for classification using linear Support Vector Machine. Having fixed the heights to 1 and 3, the cost parameter had little influence on the performance of the predictors but a cost of 1.0 returned better predictors. It was possible to build valid predictors for a single dataset. For the other three datasets the predictors did not result valid for the minority class. It is believed that the validity of the predictors depends on a minimum number of molecules in each fold of the training set in Cross Conformal Prediction. A recommendation of the number of folds is between 20 and 30 provided that each class of molecule is represented by at least 100 observations.


[Link to the report](http://uu.diva-portal.org/smash/record.jsf?dswid=-2303&pid=diva2%3A1248873&c=1&searchType=SIMPLE&language=en&query=inda&af=%5B%5D&aq=%5B%5B%5D%5D&aq2=%5B%5B%5D%5D&aqe=%5B%5D&noOfRows=50&sortOrder=author_sort_asc&sortOrder2=title_sort_asc&onlyFullText=false&sf=all)
