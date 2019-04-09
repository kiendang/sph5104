with icu_age_raw as (
    select icustay_id,
        extract('epoch' from (intime - dob)) / 60.0 / 60.0 / 24.0 / 365.242 as age
    from icustays
    left join patients using (subject_id)
)

, icu_age as (
    select icustay_id,
        case when age >= 130 then 91.5 else age end as age
    from icu_age_raw
)

, icu_order as (
    select icustay_id,
        rank() over (partition by subject_id order by intime) as icu_order
    from icustays
)

, gender as (
    select subject_id, gender
    from patients
)

, ethnicity as (
    select hadm_id, ethnicity
    from admissions
)

, angus_group as (
    select icustay_id, explicit_sepsis
    from icustays
    left join angus_sepsis using (hadm_id)
)

, diagnosis as (
    select icustay_id, substring(lower(icd9_code) from 1 for 3) as icd9_code
    from icustays left join diagnoses_icd using (hadm_id)
)

, infection0 as (
    select distinct hadm_id,
        first_value(icd9_code) over (partition by hadm_id order by seq_num) as icd9_code
    from diagnoses_icd
    where icd9_code in ('480', '481', '482', '483', '484', '485', '486', '487', '488')
)

, infection1 as (
    select hadm_id, icd9_code, short_title
    from infection0 left join d_icd_diagnoses using (icd9_code)
)

, infection as (
    select icu.icustay_id,
        case when inf.hadm_id is null then 0 else 1 end as infection,
        inf.short_title as infection_type, inf.icd9_code as infection_icd9
    from icustays icu left join infection1 inf on icu.hadm_id = inf.hadm_id
)

, cancer as (
    select icustay_id,
        case when bool_or(icd9_code between '140' and '239') then 1 else 0 end as cancer
    from diagnosis
    group by icustay_id
)

, organ_transplant as (
    select distinct icustay_id,
        case when bool_or(icd9_code = 'v42') then 1 else 0 end as organ_transplant
    from diagnosis
    group by icustay_id
)

, hiv as (
    select distinct icustay_id,
        case when bool_or(icd9_code in ('042', 'v08')) then 1 else 0 end as hiv
    from diagnosis
    group by icustay_id
)

, cyclosporine0 as (
    select distinct icustay_id from prescriptions
    where drug ilike '%cyclosporine%'
)

, cyclosporine as (
    select icu.icustay_id,
        case when pres.icustay_id is null then 0 else 1 end as cyclosporine
    from icustays icu left join cyclosporine0 pres on icu.icustay_id = pres.icustay_id
)

, methotrexate0 as (
    select distinct icustay_id from prescriptions
    where drug ilike '%methotrexate%'
)

, methotrexate as (
    select icu.icustay_id,
        case when pres.icustay_id is null then 0 else 1 end as methotrexate
    from icustays icu left join methotrexate0 pres on icu.icustay_id = pres.icustay_id
)

, mycophenolate0 as (
    select distinct icustay_id from prescriptions
    where drug ilike '%mycophenolate%'
)

, mycophenolate as (
    select icu.icustay_id,
        case when pres.icustay_id is null then 0 else 1 end as mycophenolate
    from icustays icu left join mycophenolate0 pres on icu.icustay_id = pres.icustay_id
)

, lab0 as (
    select hadm_id,
        case when itemid in (51300,51301) then 'wbc'
             when itemid in (50811,51222) then 'hemoglobin'
        else null end as label,
        valuenum as value, charttime
    from labevents
)

, lab1 as (
    select icustay_id, label,
        case when label = 'wbc' and value between 4.5 and 10 then 0
             when label = 'hemoglobin' and gender = 'M' and value between 13.8 and 17.2 then 0
             when label = 'hemoglobin' and gender = 'F' and value between 12.1 and 15.1 then 0
        else value end as value,
        charttime
    from (select * from icustays left join patients using (subject_id)) icu
    left join lab0 using (hadm_id)
    where charttime between intime and intime + interval '1 day'
        and charttime between intime and outtime
        and label is not null
)

, lab2 as (
    select *,
        first_value(value) over (partition by icustay_id, label order by charttime) as fst_val
    from lab1
    where value is not null and value > 0
)

, lab as (
    select icustay_id,
        max(case when label = 'wbc' then fst_val else null end) as wbc,
        max(case when label = 'hemoglobin' then fst_val else null end) as hemoglobin
    from lab2
    group by icustay_id
)

, mort as (
    select hadm_id,
        coalesce(adm.deathtime, pat.dod, null) as deathtime
    from admissions adm
    left join patients pat using (subject_id)
)

, mort_28 as (
    select icustay_id,
        case when deathtime <= (intime + interval '28' day) then 1 else 0 end as mortality_28_days
    from icustays left join mort using (hadm_id)
)

, elixhauser as (
    select hadm_id, elixhauser_vanwalraven, elixhauser_sid29, elixhauser_sid30
    from elixhauser_ahrq_score
)

, population as (
    select *
    from (select distinct subject_id, hadm_id, icustay_id, first_careunit, intime, outtime from icustays) a
    left join icu_order using (icustay_id)
    left join icu_age using (icustay_id)
    left join gender using (subject_id)
    left join ethnicity using (hadm_id)
    left join mort_28 using (icustay_id)
    left join elixhauser using (hadm_id)
    left join angus_group using (icustay_id)
    left join cancer using (icustay_id)
    left join organ_transplant using (icustay_id)
    left join hiv using (icustay_id)
    left join lab using (icustay_id)
    left join cyclosporine using (icustay_id)
    left join methotrexate using (icustay_id)
    left join mycophenolate using (icustay_id)
    left join infection using (icustay_id)
)

select * from population;
