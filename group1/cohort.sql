select *
from population
where icu_order = 1
and explicit_sepsis = 1
and age between 21 and 64
and hiv != 1
and cancer != 1
and organ_transplant != 1
and cyclosporine != 1
and methotrexate != 1
and mycophenolate != 1
and wbc > 4
