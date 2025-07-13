def get_archive_dest(filename: str):
    mappings = {
        "kpmgukdsp": {
            "audit_log": ("sap-dsp", "audit"),
            "event_log": ("sap-dsp", "event"),
            "app_log": ("sap-dsp", "spaces-db")
        },
        "kpmgukcis": {
            "audit_log": ("sap-cis", "audit"),
            "event_log": ("sap-cis", "event")
        },
        "kpmgukiag": {
            "audit_log": ("sap-iag", "audit"),
            "event_log": ("sap-iag", "event")
        },
        "kpmgukmrm": {
            "audit_log": ("sap-mrm", "audit"),
            "event_log": ("sap-mrm", "event")
        },
        "kpmguks4": {
            "audit_log": ("sap-btp-abap", "audit"),
            "event_log": ("sap-btp-abap", "event")
        }
    }

    fname = filename.lower()
    for tenant, log_types in mappings.items():
        for log_type, (container, folder) in log_types.items():
            if fname.startswith(f"{log_type}_{tenant}"):
                return container, folder
    return "other", "misc"