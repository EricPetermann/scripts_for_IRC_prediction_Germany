In this repository you can find R files used for generating predictions and aggregating rfesults for indoor radon prediction in Germany.

1) Model building (model_building.R) includes predictor selection, hyper parameter tuning and training of the final model. The file Croos-validation fold_IRC.R is required to define spatial blocks for cross-validation. 
2) Prediction on the floor level in each building (IRC prediction individual buildings.R) is made for the full residential building stock in Germany (~21 million buildings, ~80 million floor level predictions). Probabilistic sampling finally creates ~800 million random samples from ~80 million probability density functions fitted for each floor level.
3) Individual predictions are aggregated and different summary statistics are computed for different administrative units (national, federal states, districts, municipalities) by combining random samples created in 2) using an unique ID as identifier 
