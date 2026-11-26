"""Run the entrypoint's boot sequence with test-owned function doubles."""


def boot_script(source, doubles):
    definitions, boundary, boot = source.partition(
        "# A boot step failing because a supervised process stopped"
    )
    assert boundary, "entrypoint boot boundary is missing"
    return definitions + "\n" + doubles + "\n# " + boundary + boot
