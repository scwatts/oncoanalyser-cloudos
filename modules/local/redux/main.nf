process REDUX {
    tag "${meta.id}"
    label 'process_medium'

//    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
//        'https://depot.galaxyproject.org/singularity/hmftools-mark-dups:1.1.7--hdfd78af_0' :
//        'biocontainers/hmftools-mark-dups:1.1.7--hdfd78af_0' }"

    container "quay.io/local/hmftools-redux"

    input:
    tuple val(meta), path(bams), path(bais)
    path genome_fasta
    val genome_ver
    path genome_fai
    path genome_dict
    path unmap_regions
    path msi_jitter_sites
    val has_umis
    val umi_duplex_delim

    output:
    tuple val(meta), path('redux/')                                          , emit: redux_dir
    tuple val(meta), path('redux/*.redux.bam'), path('redux/*.redux.bam.bai'), emit: bam
    path 'versions.yml'                                                      , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''

    def umi_flags
    if(has_umis) {
        umi_flags = '-umi_enabled'
        if(umi_duplex_delim) {
            umi_flags = "${umi_flags} -umi_duplex -umi_duplex_delim ${umi_duplex_delim}"
        }
    } else {
        umi_flags = '-form_consensus'
    }

    """
    mkdir -p redux/

    redux \\
        -Xmx${Math.round(task.memory.bytes * 0.95)} \\
        ${args} \\
        -sample ${meta.sample_id} \\
        -input_bam ${bams.join(',')} \\
        -output_dir redux/ \\
        -output_bam redux/${meta.sample_id}.redux.bam \\
        -ref_genome ${genome_fasta} \\
        -ref_genome_version ${genome_ver} \\
        -unmap_regions ${unmap_regions} \\
        -ref_genome_msi_file ${msi_jitter_sites} \\
        -bamtool \$(which sambamba) \\
        ${umi_flags} \\
        -write_stats \\
        -threads ${task.cpus} \\
        -log_level DEBUG

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        redux: \$(redux -version | awk '{ print \$NF }')
        sambamba: \$(sambamba --version 2>&1 | egrep '^sambamba' | head -n 1 | awk '{ print \$NF }')
    END_VERSIONS
    """

    stub:
    """
    mkdir -p redux/

    touch redux/${meta.sample_id}.markdups.bam
    touch redux/${meta.sample_id}.markdups.bam.bai
    touch redux/${meta.sample_id}.duplicate_freq.tsv
    touch redux/${meta.sample_id}.jitter_params.tsv
    touch redux/${meta.sample_id}.ms_table.tsv.gz
    touch redux/${meta.sample_id}.repeat.tsv.gz

    if [[ -n "${has_umis}" ]]; then
        touch redux/${meta.sample_id}.umi_coord_freq.tsv
        touch redux/${meta.sample_id}.umi_edit_distance.tsv
        touch redux/${meta.sample_id}.umi_nucleotide_freq.tsv
    fi;

    echo -e '${task.process}:\\n  stub: noversions\\n' > versions.yml
    """
}
