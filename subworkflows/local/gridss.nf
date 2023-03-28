//
// GRIDSS is a software suite containing tools useful for the detection of genomic rearrangements.
//
import Constants
import Utils

include { GRIDSS_ASSEMBLE          } from '../../modules/local/gridss/assemble/main'
include { GRIDSS_CALL              } from '../../modules/local/gridss/call/main'
include { GRIDSS_PREPROCESS        } from '../../modules/local/gridss/preprocess/main'

workflow GRIDSS {
    take:
        ch_inputs                       // channel: [val(meta), tumor_bam, normal_bam]
        gridss_config                   //    file: /path/to/gridss_config (optional)
        ref_data_genome_fasta           //    file: /path/to/genome_fasta
        ref_data_genome_fai             //    file: /path/to/genome_fai
        ref_data_genome_dict            //    file: /path/to/genome_dict
        ref_data_genome_bwa_index       //    file: /path/to/genome_bwa_index_dir/
        ref_data_genome_bwa_index_image //    file: /path/to/genome_bwa_index_image
        ref_data_genome_gridss_index    //    file: /path/to/genome_gridss_index
        ref_data_gridss_blacklist       //     val: /path/to/gridss_blacklist

    main:
        // Channel for version.yml files
        ch_versions = Channel.empty()

        // Build a channel of individual BAMs for preprocessing
        // channel: [val(meta_gridss), bam]
        ch_preprocess_inputs = ch_inputs
            .flatMap { meta, tbam, nbam ->
                def sample_types = [Constants.SampleType.TUMOR, Constants.SampleType.NORMAL]
                sample_types
                    .collect { sample_type ->
                        def bam_fp
                        if (sample_type == Constants.SampleType.TUMOR) {
                            bam_fp = tbam
                        } else if (sample_type == Constants.SampleType.NORMAL) {
                            bam_fp = nbam
                        } else {
                            assert false : "got bad sample type"
                        }
                        def meta_gridss = [
                            id: meta.get(['sample_name', sample_type, Constants.SequenceType.WGS]),
                            // NOTE(SW): must use string representation for caching purposes
                            sample_type_str: sample_type.name(),
                            subject_id: meta.id,
                        ]
                        return [meta_gridss, bam_fp]
                    }
            }

        // Preprocess reads
        GRIDSS_PREPROCESS(
            ch_preprocess_inputs,
            gridss_config,
            ref_data_genome_fasta,
            ref_data_genome_fai,
            ref_data_genome_dict,
            ref_data_genome_bwa_index,
            ref_data_genome_bwa_index_image,
            ref_data_genome_gridss_index,
        )
        ch_versions = ch_versions.mix(GRIDSS_PREPROCESS.out.versions)

        // Gather BAMs and outputs from preprocessing for each tumor/normal set
        // channel: [subject_id, [[val(meta_gridss), bam, preprocess_dir], ...]]
        ch_bams_and_preprocess = WorkflowOncoanalyser.groupByMeta(
            ch_preprocess_inputs,
            GRIDSS_PREPROCESS.out.preprocess_dir,
        )
        .map { [it[0].subject_id, it] }
        .groupTuple(size: 2)

        // Order and organise inputs for assembly
        // channel: [val(meta_gridss), [bams], [preprocess_dirs], [labels]]
        ch_assemble_inputs = ch_bams_and_preprocess
            .map { subject_id, entries ->
                def (tmeta, tbam, tpreprocess) = get_sample_type_entry(entries, Constants.SampleType.TUMOR)
                def (nmeta, nbam, npreprocess) = get_sample_type_entry(entries, Constants.SampleType.NORMAL)
                def meta_gridss = [id: tmeta.subject_id]
                return [meta_gridss, [nbam, tbam], [npreprocess, tpreprocess], [nmeta.id, tmeta.id]]
            }

        // Assemble variants
        GRIDSS_ASSEMBLE(
            ch_assemble_inputs,
            gridss_config,
            ref_data_genome_fasta,
            ref_data_genome_fai,
            ref_data_genome_dict,
            ref_data_genome_bwa_index,
            ref_data_genome_bwa_index_image,
            ref_data_genome_gridss_index,
            ref_data_gridss_blacklist,
        )
        ch_versions = ch_versions.mix(GRIDSS_ASSEMBLE.out.versions)

        // Prepare inputs for variant calling
        // channel: [val(meta_gridss), [bams], assemble_dir, [labels]]
        ch_call_inputs = WorkflowOncoanalyser.groupByMeta(
            ch_assemble_inputs,
            GRIDSS_ASSEMBLE.out.assemble_dir,
            flatten: false,
        )
            .map { data ->
                def meta = data[0]
                def (bams, preprocess_dirs, labels) = data[1]
                def (assemble_dir) = data[2]
                return [meta, bams, assemble_dir, labels]
            }

        // Call variants
        GRIDSS_CALL(
            ch_call_inputs,
            gridss_config,
            ref_data_genome_fasta,
            ref_data_genome_fai,
            ref_data_genome_dict,
            ref_data_genome_bwa_index,
            ref_data_genome_bwa_index_image,
            ref_data_genome_gridss_index,
            ref_data_gridss_blacklist,
        )
        ch_versions = ch_versions.mix(GRIDSS_CALL.out.versions)

        // Reunite final VCF with the corresponding input meta object
        ch_out = Channel.empty()
            .concat(
                ch_inputs.map { meta, tbam, nbam -> [meta.id, meta] },
                GRIDSS_CALL.out.vcf.map { meta, vcf -> [meta.id, vcf] },
            )
            .groupTuple(size: 2)
            .map { id, other -> other.flatten() }

    emit:
        results  = ch_out      // channel: [val(meta), vcf]

        versions = ch_versions // channel: [versions.yml]
}

def get_sample_type_entry(entries, sample_type) {
    entries.find { e ->
        def meta = e[0]
        return Utils.getEnumFromString(meta.sample_type_str, Constants.SampleType) == sample_type
    }
}
