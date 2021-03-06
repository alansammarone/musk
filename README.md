# Musk

[![Build Status](https://travis-ci.com/alansammarone/musk.svg?branch=master)](https://travis-ci.com/alansammarone/musk)
[![codecov](https://codecov.io/gh/alansammarone/musk/branch/master/graph/badge.svg)](https://codecov.io/gh/alansammarone/musk)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)




This package contains a collection of classes, algoritms and plots used in my Bachelor thesis work on computation thesis. It's goal is to make sure all of the results presented are easily reproducible. See https://github.com/alansammarone/MandelbrotPercolation.




## Table of Contents

- [Installation](#installation)

- [Examples](#examples)


## Installation
To install this package you can run `pip install musk`.

## Examples

#### 2D Percolation

This is just the classic percolation, in which we start with a square lattice of size N, and randomly set the state of each node to 1 with probability `p` and 0 with probability `1-p`. Some interesting quantities to look at are, for example, the average cluster size as a function of `p`:

![perc_2d_cluster_size](examples/images/perc_2d_cluster_size.png)  

as well as the percolation probability, i.e., the probability that there exists a cluster connection the top and bottom edges of the lattice, as a function of the occupation probability `p`:

![perc_2d_prob](examples/images/perc_2d_prob.png)

#### Mandelbrot percolation

This is a type of percolation in which we subdivide each lattice site into sub-sites (normally each site becomes 4) multiple times. Each time, with probability *p*, we color each of the not yet colored sites. We end up with a self similar, fractal structure - hence the name.

The [percolation_mandelbrot_image.py](examples/percolation_mandelbrot_image.py) example produces an image of the lattice:

![mandelbrot_percolation](examples/images/mandelbrot_percolation.png)




