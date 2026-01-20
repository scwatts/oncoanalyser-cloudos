process SAMTOOLS_VIEW {
    tag "${meta.id}"
    label 'process_single'
    label 'process_medium_memory'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/samtools:1.22.1--h96c455f_0' :
        'biocontainers/samtools:1.22.1--h96c455f_0' }"

    input:
    tuple val(meta), path(cram), path(crai)
    path genome_fasta
    path genome_fai

    output:
    tuple val(meta), path('*.bam'), path('*.bai'), emit: fastq
    path 'versions.yml'                          , emit: versions
    path '.command.*'                            , emit: command_files

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def args2 = task.ext.args2 ?: ''

    """
    samtools view \\
        ${args} \\
        --bam \\
        --reference ${genome_fasta} \\
        --threads ${task.cpus} \\
        --write-index \\
        --output ${meta.id}.bam \\
        ${cram}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(samtools --version | sed -n '/^samtools / { s/^.* //p }')
    END_VERSIONS
    """

    stub:
    """
    touch ${meta.id}.bam
    touch ${meta.id}.bai

    echo -e '${task.process}:\\n  stub: noversions\\n' > versions.yml
    """
}
