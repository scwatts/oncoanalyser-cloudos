process SAMTOOLS_FASTQ {
    tag "${meta.id}"
    label 'process_single'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/samtools:1.22.1--h96c455f_0' :
        'biocontainers/samtools:1.22.1--h96c455f_0' }"

    input:
    tuple val(meta), path(cram), path(crai)
    path genome_fasta
    path genome_fai

    output:
    tuple val(meta), path('*R1.fastq.gz'), path('*R2.fastq.gz'), emit: fastq
    path 'versions.yml'                                        , emit: versions
    path '.command.*'                                          , emit: command_files

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''

    """
    samtools fastq \\
        ${args} \\
        -0 ${meta.id}.other.fastq.gz \\
        -1 ${meta.id}.R1.fastq.gz \\
        -2 ${meta.id}.R2.fastq.gz \\
        -s ${meta.id}.singleton.fastq.gz \\
        --reference ${genome_fasta} \\
        --threads ${task.cpus} \\
        ${cram}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(samtools --version | sed -n '/^samtools / { s/^.* //p }')
    END_VERSIONS
    """

    stub:
    """
    touch ${meta.id}.R1.fastq.gz
    touch ${meta.id}.R2.fastq.gz

    echo -e '${task.process}:\\n  stub: noversions\\n' > versions.yml
    """
}
