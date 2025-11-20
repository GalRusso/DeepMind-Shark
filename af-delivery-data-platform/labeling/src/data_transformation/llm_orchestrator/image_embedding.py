import os
import tempfile
from collections.abc import Callable
from logging import getLogger
from typing import Any, cast

import numpy as np
import open_clip
import torch
import torch.nn.functional as F
from PIL import Image
from pydantic import BaseModel

logger = getLogger(__name__)


class ImageEmbeddingResult(BaseModel):
    embedding: list[float] | None
    embedding_binary: bytes | None = None
    model_name: str
    pretrained: str
    used_text: bool
    description: str | None = None
    tags: list[str] | None = None


class CLIPClient:
    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        logger.info("Initializing CLIPClient...")

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")

        logger.info(f"Loading CLIP model: {model_name} with weights: {pretrained}")
        self.model, _, self.preprocess_val = open_clip.create_model_and_transforms(model_name, pretrained=pretrained)
        self.model.to(self.device).eval()

        self.model_name = model_name
        self.pretrained = pretrained

        logger.info("CLIP model loaded successfully.")

    def initialize(self) -> None:
        logger.info("Running CLIPClient initialization checks...")

        try:
            self._check_text_embedding()
            logger.info("Text embedding check passed.")
        except Exception as error:
            raise RuntimeError(f"Text embedding failed: {error}") from error

        try:
            self._check_image_embedding()
            logger.info("Image embedding check passed.")
        except Exception as error:
            raise RuntimeError(f"Image embedding failed: {error}") from error

        logger.info("CLIPClient is fully initialized and functional.")

    def embed_text(self, text: str) -> list[float]:
        if not text:
            raise ValueError("Text input is empty.")

        with torch.no_grad():
            tokens: torch.Tensor = open_clip.tokenize([text]).to(self.device)  # type: ignore[reportGeneralTypeIssues]
            features: torch.Tensor = self.model.encode_text(tokens)  # type: ignore[reportGeneralTypeIssues]
            normalized: torch.Tensor = F.normalize(features, dim=-1)  # type: ignore[reportGeneralTypeIssues]
            return normalized.squeeze().cpu().numpy().astype(np.float32).tolist()

    def embed_image(self, image_path: str) -> list[float]:
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"Image file not found: {image_path}")

        image = Image.open(image_path).convert("RGB")
        preprocess_fn: Callable[[Image.Image], torch.Tensor] = cast(Any, self.preprocess_val)
        image_tensor: torch.Tensor = preprocess_fn(image)  # type: ignore[reportGeneralTypeIssues]
        image_tensor = image_tensor.unsqueeze(0).to(self.device)

        with torch.no_grad():
            features: torch.Tensor = self.model.encode_image(image_tensor)  # type: ignore[reportGeneralTypeIssues]
            normalized: torch.Tensor = F.normalize(features, dim=-1)  # type: ignore[reportGeneralTypeIssues]
            return normalized.squeeze().cpu().numpy().astype(np.float32).tolist()

    @staticmethod
    def average_embeddings(embeddings: list[list[float] | None]) -> list[float] | None:
        valid = [e for e in embeddings if isinstance(e, list) and e is not None]

        if not valid:
            return None

        array = np.array(valid, dtype=np.float32)

        if array.ndim != 2 or array.shape[1] != 512:
            raise ValueError(f"Expected embeddings of shape [N, 512], got {array.shape}")

        avg_vec = array.mean(axis=0)
        norm = np.linalg.norm(avg_vec)

        if norm == 0:
            raise ValueError("Cannot normalize a zero vector.")

        return (avg_vec / norm).tolist()

    def _check_text_embedding(self) -> None:
        dummy_text = "a test sentence"
        embedding = self.embed_text(dummy_text)
        if len(embedding) != 512:
            raise ValueError("Text embedding has incorrect dimension.")

    def _check_image_embedding(self) -> None:
        blank_image = Image.new("RGB", (224, 224), (255, 255, 255))
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
            test_path = tmp_file.name
            blank_image.save(test_path)

        embedding = self.embed_image(test_path)
        if len(embedding) != 512:
            raise ValueError("Image embedding has incorrect dimension.")

        os.remove(test_path)

    def run(self, *, file_path: str, description: str | None, tags: list[str] | None) -> ImageEmbeddingResult:
        text: str | None = None
        if description or tags:
            parts: list[str] = []
            if tags:
                parts.extend([str(t) for t in tags if t is not None])
            if description:
                parts.append(str(description))
            if parts:
                text = " ".join(parts)

        embeddings: list[list[float] | None] = []
        if text:
            text_embedding = self.embed_text(text)
            embeddings.append(text_embedding)
        image_embedding = self.embed_image(file_path)
        embeddings.append(image_embedding)
        embedding = CLIPClient.average_embeddings(embeddings)

        # serialize to compact float32 bytes for storage
        embedding_binary: bytes | None = None
        if embedding is not None:
            arr = np.asarray(embedding, dtype=np.float32)
            if arr.ndim != 1 or arr.size != 512:
                raise ValueError(f"Invalid embedding shape: {arr.shape}")
            embedding_binary = arr.tobytes()

        return ImageEmbeddingResult(
            embedding=embedding,
            embedding_binary=embedding_binary,
            model_name=self.model_name,
            pretrained=self.pretrained,
            used_text=bool(text),
            description=description,
            tags=tags,
        )

    def batch(
        self,
        file_paths: list[str],
        descriptions: list[str | None],
        tags_list: list[list[str] | None],
    ) -> list[ImageEmbeddingResult]:
        if not (len(file_paths) == len(descriptions) == len(tags_list)):
            raise ValueError("Input lists must have the same length")
        results: list[ImageEmbeddingResult] = []
        for file_path, description, tags in zip(file_paths, descriptions, tags_list, strict=False):
            results.append(self.run(file_path=file_path, description=description, tags=tags))
        return results


if __name__ == "__main__":
    client = CLIPClient()
    client.initialize()
