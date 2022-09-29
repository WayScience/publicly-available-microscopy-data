# publicly-available-microscopy-data
Compiling and describing publicly available microscopy datasets

## Statistics
* Richness (S)
    * The number of unique entries within an image attribute
* Shannon Index (H')
    * A measure of diversity in a single community
    * Considers richness and evenness
    * Ranges [0, $\infty$]
    * $$ H'=-\sum\limits_{i=1}^S p_{i}lnp_{i} $$
* Normalized Median Evenness (NME)
    * Normalized estimate of evenness taken from the median of -p_iln(p_i) from Shannon Index calculations
* Pielou's Evenness (J')
    * Ratio of observed H' to the maximum possible H' given a sample
* Simpson's Evennnes (E)
    * 
* Gini Coefficient (GC)