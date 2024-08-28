//
// Convert CRAM to BAM
//

import Constants
import Utils

include { CUSTOM_CRAM_BAM_CONVERT as CRAM_BAM_CONVERT } from '../../../modules/local/custom/cram_bam_convert/main'

workflow CRAM_BAM_CONVERSION {
    take:
    // Sample data
    ch_inputs              // channel: [mandatory] [ meta ]

    // Reference data
    genome_fasta           // channel: [mandatory] /path/to/genome_fasta

    main:
    // Channel for version.yml files
    // channel: [ versions.yml ]
    ch_versions = Channel.empty()

    // Sort inputs, separate by tumor and normal
    // channel: [ meta ]
    ch_inputs_tumor_sorted = ch_inputs
        .branch { meta ->
            // NOTE(SW): no existing check since we explicitly disallow this
            runnable: Utils.hasTumorDnaCram(meta)
            skip: true
        }

    ch_inputs_normal_sorted = ch_inputs
        .branch { meta ->
            // NOTE(SW): no existing check since we explicitly disallow this
            runnable: Utils.hasNormalDnaCram(meta)
            skip: true
        }

    //
    // MODULE: CRAM to BAM conversion
    //
    // Create process input channel
    // channel: [ meta_cram, cram ]
    ch_cram_inputs = Channel.empty()
        .mix(
            ch_inputs_tumor_sorted.runnable.map { meta -> [meta, Utils.getTumorDnaSample(meta), 'tumor'] },
            ch_inputs_normal_sorted.runnable.map { meta -> [meta, Utils.getNormalDnaSample(meta), 'normal'] },
        )
        .map { meta, meta_sample, sample_type ->

            def meta_cram = [
                key: meta.group_id,
                id: "${meta.group_id}_${meta_sample.sample_id}",
                sample_id: meta_sample.sample_id,
                sample_type: sample_type,
            ]

            def cram = meta_sample.getAt(Constants.FileType.CRAM)
            return [meta_cram, cram]
        }

    // Run process
    CRAM_BAM_CONVERT(
        ch_cram_inputs,
        genome_fasta,
    )

    // Combine BAM and BAI then sort into tumor and normal channels
    // channel: [ meta_group, bam, bai ]
    ch_bams_combined = WorkflowOncoanalyser.groupByMeta(
        CRAM_BAM_CONVERT.out.bam,
        CRAM_BAM_CONVERT.out.bai,
    )
        .branch { meta_cram, bam, bai ->
            assert ['tumor', 'normal'].contains(meta_cram.sample_type)
            tumor: meta_cram.sample_type == 'tumor'
            normal: meta_cram.sample_type == 'normal'
            placeholder: true
        }

    // Set outputs, restoring original meta
    // channel: [ meta, bam ]
    ch_bam_tumor_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_bams_combined.tumor, ch_inputs),
            ch_inputs_tumor_sorted.skip.map { meta -> [meta, [], []] },
        )

    ch_bam_normal_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_bams_combined.normal, ch_inputs),
            ch_inputs_normal_sorted.skip.map { meta -> [meta, [], []] },
        )

    emit:
    dna_tumor  = ch_bam_tumor_out  // channel: [ meta, [bam, ...], [bai, ...] ]
    dna_normal = ch_bam_normal_out // channel: [ meta, [bam, ...], [bai, ...] ]

    versions   = ch_versions       // channel: [ versions.yml ]
}
