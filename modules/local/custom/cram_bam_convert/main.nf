process CUSTOM_CRAM_BAM_CONVERT {
    tag "${meta.id}"
    label 'process_single'

    conda (params.enable_conda ? "bioconda::samtools=1.19.2" : null)
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/samtools:1.19.2--h50ea8bc_1' :
        'biocontainers/samtools:1.19.2--h50ea8bc_1' }"

    input:
    tuple val(meta), path(cram)
    path genome_fasta
    path genome_fai

    output:
    tuple val(meta), path("*bam"), emit: bam
    tuple val(meta), path("*bai"), emit: bai
    path "versions.yml"          , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''

    """
    samtools view --threads ${task.cpus} --reference ${genome_fasta} --output ${cram.baseName}.bam ${cram}
    samtools index --threads ${task.cpus} ${cram.baseName}.bam

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        samtools: \$(echo \$(samtools --version 2>&1) | sed 's/^.*samtools //; s/Using.*\$//')
    END_VERSIONS
    """

    stub:
    """
    touch ${cram.baseName}.bam
    touch ${cram.baseName}.bam.bai

    echo -e '${task.process}:\\n  stub: noversions\\n' > versions.yml
    """
}
