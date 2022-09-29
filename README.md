# publicly-available-microscopy-data
Compiling and describing publicly available microscopy datasets

## Statistics
* Richness (S)
    * The number of unique entries within an image attribute
* Shannon Index (H')
    * A measure of diversity in a single community
    * Considers richness and evenness
    * Ranges [0, $\infty$]
    * $H'=-\sum\limits_{i=1}^S p_{i}lnp_{i}$
* Normalized Median Evenness (NME)
    * Normalized estimate of evenness taken from the median of -p_iln(p_i) from Shannon Index calculations
    * $NME = \frac{median(-p_{i}lnp_{i})}{max(-p_{i}lnp_{i})}$
* Pielou's Evenness (J')
    * Ratio of observed H' to the maximum possible H' given a sample
    * $J'=\frac{H'}{lnS}$
* Simpson's Evennnes (E)
    * $E=\frac{1/D}{S}$
* Gini Coefficient (GC)
  * Measure of inequality within a community
  * Range [0, 1] where 1 is absolute inequality and 0 is perfect equality
  * $GC=\frac{A}{(A+B)}$
    * `A` = Area above Lorenze curve
    * `B` = Area below Lorenze curve

