import warnings

# Keep module importable for legacy callers, but signal removal at runtime.
warnings.warn(
    "The Bing cognitive services have been removed from SynapseML; the "
    "'synapse.ml.cognitive.bing' compatibility module will be deleted in a future release.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = []


def __getattr__(name: str):
    """Inform callers that the legacy Bing APIs are no longer available."""

    raise ImportError(
        "Attribute '{0}' is unavailable because the Bing services were removed from SynapseML.".format(name)
    )
