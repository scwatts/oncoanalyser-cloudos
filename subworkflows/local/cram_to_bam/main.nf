//
// Convert CRAM to BAM
//

import Constants
import Utils

include { SAMTOOLS_VIEW } from '../../../modules/local/samtools/view'

workflow CRAM_TO_BAM {
    take:
    // Sample data
    ch_inputs    // channel: [mandatory] [ meta ]

    // Reference data
    genome_fasta // channel: [mandatory] /path/to/genome_fasta
    genome_fai   // channel: [mandatory] /path/to/genome_fai

    main:
    // Channel for version.yml files
    // channel: [ versions.yml ]
    ch_versions = Channel.empty()

    // Select and sort input sources, separating bytumor and normal
    // channel: runnable: [ meta, [cram, ...], [crai, ...] ]
    // channel: skip: [ meta ]
    ch_inputs_tumor_sorted = ch_inputs
        .map { meta ->
            return [
                meta,
                Utils.hasTumorCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAM_DNA_TUMOR)] : [],
                Utils.hasTumorCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAI_DNA_TUMOR)] : [],
            ]
        }
        .branch { meta, crams, crais ->
            runnable: crams
            skip: true
                return meta
        }

    ch_inputs_normal_sorted = ch_inputs
        .map { meta ->
            return [
                meta,
                Utils.hasNormalCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAM_DNA_NORMAL)] : [],
                Utils.hasNormalCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAI_DNA_NORMAL)] : [],
            ]
        }
        .branch { meta, crams, crais ->
            runnable: crams
            skip: true
                return meta
        }

    ch_inputs_donor_sorted = ch_inputs
        .map { meta ->
            return [
                meta,
                Utils.hasDonorCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAM_DNA_DONOR)] : [],
                Utils.hasDonorCramConvertDna(meta) ? [Utils.getInput(meta, Constants.INPUT.BAI_DNA_DONOR)] : [],
            ]
        }
        .branch { meta, crams, crais ->
            runnable: crams
            skip: true
            return meta
        }

    // Create process input channel
    // channel: [ meta_samtools, [cram, ...], [crai, ...] ]
    ch_samtools_inputs = Channel.empty()
        .mix(
            ch_inputs_tumor_sorted.runnable.map { meta, crams, crais -> [meta, Utils.getTumorDnaSample(meta), 'tumor', crams, crais] },
            ch_inputs_normal_sorted.runnable.map { meta, crams, crais -> [meta, Utils.getNormalDnaSample(meta), 'normal', crams, crais] },
            ch_inputs_donor_sorted.runnable.map { meta, crams, crais -> [meta, Utils.getDonorDnaSample(meta), 'donor', crams, crais] },
        )
        .map { meta, meta_sample, sample_type, crams, crais ->

            def sample_id = meta_sample.getOrDefault('longitudinal_sample_id', meta_sample['sample_id'])

            def meta_samtools = [
                key: meta.group_id,
                id: "${meta.group_id}_${sample_id}",
                sample_id: sample_id,
                sample_type: sample_type,
            ]

            return [meta_samtools, crams, crais]
        }

    // Run process
    SAMTOOLS_VIEW(
        ch_samtools_inputs,
        genome_fasta,
        genome_fai,
    )

    ch_versions = ch_versions.mix(SAMTOOLS_VIEW.out.versions)

    // Sort into a tumor and normal channel
    // channel: [ meta_samtools, cram, crai ]
    ch_samtools_out_sorted = SAMTOOLS_VIEW.out.fastq
        .branch { meta_samtools, fastq_fwd, fastq_rev ->
            assert ['tumor', 'normal', 'donor'].contains(meta_samtools.sample_type)
            tumor: meta_samtools.sample_type == 'tumor'
            normal: meta_samtools.sample_type == 'normal'
            donor: meta_samtools.sample_type == 'donor'
            placeholder: true
        }

    // Set outputs, restoring original meta
    // channel: [ meta, bam, bai]
    ch_samtools_tumor_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_samtools_out_sorted.tumor, ch_inputs),
            ch_inputs_tumor_sorted.skip.map { meta -> [meta, [], []] },
        )

    ch_samtools_normal_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_samtools_out_sorted.normal, ch_inputs),
            ch_inputs_normal_sorted.skip.map { meta -> [meta, [], []] },
        )

    ch_samtools_donor_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_samtools_out_sorted.donor, ch_inputs),
            ch_inputs_donor_sorted.skip.map { meta -> [meta, [], []] },
        )

    emit:
    dna_tumor  = ch_samtools_tumor_out  // channel: [ meta, bam, bai ]
    dna_normal = ch_samtools_normal_out // channel: [ meta, bam, bai ]
    dna_donor  = ch_samtools_donor_out  // channel: [ meta, bam, bai ]

    versions   = ch_versions            // channel: [ versions.yml ]
}
