//
// This file holds several Groovy functions that could be useful for any Nextflow pipeline
//

import org.yaml.snakeyaml.Yaml

import nextflow.Nextflow

class Utils {

    //
    // When running with -profile conda, warn if channels have not been set-up appropriately
    //
    public static void checkCondaChannels(log) {
        Yaml parser = new Yaml()
        def channels = []
        try {
            def config = parser.load("conda config --show channels".execute().text)
            channels = config.channels
        } catch(NullPointerException | IOException e) {
            log.warn "Could not verify conda channel configuration."
            return
        }

        // Check that all channels are present
        // This channel list is ordered by required channel priority.
        def required_channels_in_order = ['conda-forge', 'bioconda', 'defaults']
        def channels_missing = ((required_channels_in_order as Set) - (channels as Set)) as Boolean

        // Check that they are in the right order
        def channel_priority_violation = false
        def n = required_channels_in_order.size()
        for (int i = 0; i < n - 1; i++) {
            channel_priority_violation |= !(channels.indexOf(required_channels_in_order[i]) < channels.indexOf(required_channels_in_order[i+1]))
        }

        if (channels_missing | channel_priority_violation) {
            log.warn "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
                "  There is a problem with your Conda configuration!\n\n" +
                "  You will need to set-up the conda-forge and bioconda channels correctly.\n" +
                "  Please refer to https://bioconda.github.io/\n" +
                "  The observed channel order is \n" +
                "  ${channels}\n" +
                "  but the following channel order is required:\n" +
                "  ${required_channels_in_order}\n" +
                "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        }
    }

    static public getEnumFromString(s, e) {
        try {
            return e.valueOf(s.toUpperCase())
        } catch(java.lang.IllegalArgumentException err) {
            return null
        }
    }

    public static getEnumNames(e) {
        e
            .values()
            *.name()
            *.toLowerCase()
    }


    static public getFileObject(path) {
        return path ? Nextflow.file(path) : []
    }


    // Sample names
    static public getTumorWgsSampleName(meta) {
        return getMetaEntry(meta, ['sample_name', Constants.SampleType.TUMOR, Constants.SequenceType.WGS])
    }

    static public getTumorWtsSampleName(meta) {
        return getMetaEntry(meta, ['sample_name', Constants.SampleType.TUMOR, Constants.SequenceType.WTS])
    }

    static public getNormalWgsSampleName(meta) {
        return getMetaEntry(meta, ['sample_name', Constants.SampleType.NORMAL, Constants.SequenceType.WGS])
    }


    // Files
    static public getTumorWgsBam(meta) {
        return getMetaEntry(meta, [Constants.FileType.BAM, Constants.SampleType.TUMOR, Constants.SequenceType.WGS])
    }

    static public getNormalWgsBam(meta) {
        return getMetaEntry(meta, [Constants.FileType.BAM, Constants.SampleType.NORMAL, Constants.SequenceType.WGS])
    }

    static public getTumorWtsBam(meta) {
        return getMetaEntry(meta, [Constants.FileType.BAM, Constants.SampleType.TUMOR, Constants.SequenceType.WTS])
    }


    static public getMetaEntry(meta, key) {
        if (! meta.containsKey(key)) {
            System.err.println "\nERROR: meta does not contain key ${key}: ${meta}"
            System.exit(1)
        }
        return meta.getAt(key)
    }
}
