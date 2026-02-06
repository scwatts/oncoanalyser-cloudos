//
// Picard fix mate information sets the mate CIGAR string tag
//

import Constants
import Utils

include { PICARD_FIXMATEINFORMATION as FIXMATEINFORMATION } from '../../../modules/local/picard/fixmateinformation/main'

workflow PICARD_FIXMATEINFORMATION {
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

    // Sort inputs, separate by tumor and normal
    // channel: [ meta ]
    ch_inputs_tumor_sorted = ch_inputs
        .branch { meta ->
            def has_existing = Utils.hasExistingInput(meta, Constants.INPUT.BAM_REDUX_DNA_TUMOR)
            runnable: Utils.hasTumorDnaBam(meta) && !has_existing
            skip: true
        }

    ch_inputs_normal_sorted = ch_inputs
        .branch { meta ->
            def has_existing = Utils.hasExistingInput(meta, Constants.INPUT.BAM_REDUX_DNA_NORMAL)
            runnable: Utils.hasNormalDnaBam(meta) && !has_existing
            skip: true
        }

    ch_inputs_donor_sorted = ch_inputs
        .branch { meta ->
            def has_existing = Utils.hasExistingInput(meta, Constants.INPUT.BAM_REDUX_DNA_DONOR)
            runnable: Utils.hasDonorDnaBam(meta) && !has_existing
            skip: true
        }

    //
    // MODULE: picard fixmateinformation
    //
    // Create process input channel
    // channel: [ meta_picard, bam, bai ]
    ch_fixmate_inputs = Channel.empty()
        .mix(
            ch_inputs_tumor_sorted.runnable.map { meta -> [meta, Utils.getTumorDnaSample(meta), 'tumor'] },
            ch_inputs_normal_sorted.runnable.map { meta -> [meta, Utils.getNormalDnaSample(meta), 'normal'] },
            ch_inputs_donor_sorted.runnable.map { meta -> [meta, Utils.getDonorDnaSample(meta), 'donor'] },
        )
        .map { meta, meta_sample, sample_type ->
              def meta_picard = [
                  key: meta.group_id,
                  id: "${meta.group_id}_${meta_sample['sample_id']}",
                  sample_type: sample_type,
              ]

              return [meta_picard, meta_sample.getOrDefault(Constants.FileType.BAM, null), meta_sample.getOrDefault(Constants.FileType.BAI, null)]
        }

    // Run process
    FIXMATEINFORMATION(
        ch_fixmate_inputs,
        genome_fasta,
        genome_fai,
    )

    ch_versions = ch_versions.mix(FIXMATEINFORMATION.out.versions)

    // Sort BAMs
    // NOTE(SW): always expect exactly one BAM per sample; nesting within list for downstream compatibility
    // channel: [ meta_picard, [bam], [bai] ]
    ch_bams_united = FIXMATEINFORMATION.out.bam
        .map { meta_picard, bam, bai -> return [meta_picard, [bam], [bai]] }
        .branch { meta_picard, bam, bai ->
            assert ['tumor', 'normal', 'donor'].contains(meta_picard.sample_type)
            tumor: meta_picard.sample_type == 'tumor'
            normal: meta_picard.sample_type == 'normal'
            donor: meta_picard.sample_type == 'donor'
            placeholder: true
        }

    // Set outputs, restoring original meta
    // channel: [ meta, [bam], [bai] ]
    ch_bam_tumor_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_bams_united.tumor, ch_inputs),
            ch_inputs_tumor_sorted.skip.map { meta -> [meta, [], []] },
        )

    ch_bam_normal_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_bams_united.normal, ch_inputs),
            ch_inputs_normal_sorted.skip.map { meta -> [meta, [], []] },
        )

    ch_bam_donor_out = Channel.empty()
        .mix(
            WorkflowOncoanalyser.restoreMeta(ch_bams_united.donor, ch_inputs),
            ch_inputs_donor_sorted.skip.map { meta -> [meta, [], []] },
        )

    emit:
    dna_tumor  = ch_bam_tumor_out  // channel: [ meta, [bam], [bai] ]
    dna_normal = ch_bam_normal_out // channel: [ meta, [bam], [bai] ]
    dna_donor  = ch_bam_donor_out  // channel: [ meta, [bam], [bai] ]

    versions   = ch_versions       // channel: [ versions.yml ]
}
