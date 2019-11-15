with species_count as (
    select genus, count(*) as species_in_genus
    from taxonomy
    group by genus),
genus_count as (
    select genus, count(*) as samples_in_genus
    from taxonomy_ids
    join taxonomy using (sci_name)
    group by genus),
samples_collected as (
    select genus, count(*) as collected_in_genus
    from qc_normal_plate_layout
    join taxonomy_ids using (sample_id)
    join taxonomy using (sci_name)
    group by genus),
samples_sequenced as (
    select taxonomy.genus, count(*) as sequenced_in_genus
    from reformatting_templates
    join taxonomy_ids using (sample_id)
    join taxonomy using (sci_name)
    group by taxonomy.genus
)
select distinct taxonomy_ids.sample_id, taxonomy.genus, taxonomy.sci_name,
    priority_taxa.priority,
	species_count.species_in_genus,
	genus_count.samples_in_genus,
    samples_collected.collected_in_genus,
	samples_sequenced.sequenced_in_genus,
    sequencing_metadata.loci_assembled,
	qc_normal_plate_layout.total_dna,
	qc_normal_plate_layout.rapid_id as sample_code_submitted,
	reformatting_templates.rapid_seq_id as sample_code_sequenced
from taxonomy_ids
join taxonomy using (sci_name)
left join qc_normal_plate_layout using (sample_id)
left join sequencing_metadata using (sample_id)
left join reformatting_templates using (rapid_id)
left join priority_taxa using (genus)
left join species_count using (genus)
left join genus_count using (genus)
left join samples_collected using (genus)
left join samples_sequenced using (genus)
where taxonomy_ids.sample_id not in (select sample_id from taxonomy_errors)
order by taxonomy.sci_name;
