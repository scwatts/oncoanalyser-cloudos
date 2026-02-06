process PICARD_FIXMATEINFORMATION {
    tag "${meta.id}"
    label 'process_medium'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/picard:3.3.0--hdfd78af_0' :
        'biocontainers/picard:3.3.0--hdfd78af_0' }"

    input:
    tuple val(meta), path(bam), path(bai)
    path genome_fasta
    path genome_fai

    output:
    tuple val(meta), path('*.bam'), path('*.bai'), emit: bam
    path 'versions.yml'                          , emit: versions
    path '.command.*'                            , emit: command_files

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''

    def xmx_mod = task.ext.xmx_mod ?: 0.75

    """
    picard \\
        FixMateInformation \\
        -Xmx${Math.round(task.memory.bytes * xmx_mod)} \\
        ${args} \\
        --INPUT ${bam} \\
        --REFERENCE_SEQUENCE ${genome_fasta} \\
        --ADD_MATE_CIGAR \\
        --CREATE_INDEX \\
        --OUTPUT ${meta.id}.mc.bam \\

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        picard: \$(picard FixMateInformation --version 2>&1 | sed -n '/^Version:/ { s/^.*://p }')
    END_VERSIONS
    """

    stub:
    """
    touch ${meta.id}.mc.bam
    touch ${meta.id}.mc.bam.bai

    echo -e '${task.process}:\\n  stub: noversions\\n' > versions.yml
    """
}
