# publicly-available-microscopy-data
## Purpose
Compile and describe publicly available microscopy datasets using alpha and beta diversity measures. 

We extract metadata for each well and compute diveristy statistics for image attributes including but not limited to `screen id`, `genotype`, `phenotype`, `imaging method`, `cell line`, and `organism`. 

These measures give us insight into the quality of these datasets for use in image-based profiling methods.

## Conda Environment
`cd` to your `publicly-available-microscopy-data` directory and run the following command:

```bash
conda env create -n microscopy-data
```

Activate the environment:

```bash
conda activate microscopy-data
```

## Statistics
* Alpha Diversity
  * Richness (S)
      * The number of unique entries within an image attribute
  * Shannon Index (H')
      * A measure of diversity in a single community given richness and evenness
      * Ranges [0, $\infty$] where ↑H' = high diversity and ↓H' = low diversity
      * $H'=-\sum\limits_{i=1}^S p_{i}lnp_{i}$
      * Limitations
        * H' can be similar between highly differing communities
        * Biased with small sample sizes
  * Normalized Median Evenness (NME)
      * Normalized _estimate_ of evenness taken from the median $-p_iln(p_i)$ value from Shannon Index calculations
      * Described by [Gauthier and Derome (2021)](https://journals.asm.org/doi/pdf/10.1128/msphere.01019-20)
      * $NME = \frac{median(-p_{i}lnp_{i})}{max(-p_{i}lnp_{i})}$
  * Pielou's Evenness (J')
      * Ratio of observed H' to the maximum possible H' given a sample
      * $J'=\frac{H'}{H'_{max}}$
        * Where $H'_{max}=lnS$
      * Limitations
        * Highly influenced by dominant species
        * Not independent of richness (S)
  * Simpson's Evennnes (E)
    * Ratio of inverse dominance $D$ to richness
    * $E=\frac{1/D}{S}$
    * Where $D=\sum\limits_{i=1}^S p_{i}^2$
  * Gini Coefficient (GC)
    * Measure of inequality distribution within a community
    * Ranges [0, 1] where 1 is absolute inequality and 0 is perfect equality
    * $GC=\frac{A}{(A+B)}$
      * `A` = Area above Lorenze curve
      * `B` = Area below Lorenze curve

