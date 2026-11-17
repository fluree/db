//! Fitting a crop under the gateway's request ceiling.
//!
//! A crop ships inline, base64'd into the request body. The Fluree AI
//! gateway rides a Lambda Function URL whose 6 MB request cap is an AWS
//! hard limit, and base64 inflates by a third, so the binary ceiling is
//! about 4.2 MB. The engine renders crops at a fixed 2x scale, and a dense
//! engineering drawing at 2x is ~3168x2448 px of linework that PNG cannot
//! compress: 6.65 MB for one crop, and, escalation being all-or-nothing,
//! a dead document.
//!
//! Downscaling costs nothing the model would have seen: vision APIs
//! normalise an image to roughly 1568 px on the long edge before the model
//! reads it. So anything that fits is sent untouched, and only what does
//! not is walked down the long edge until it fits or reaches a floor below
//! which the crop no longer resembles what the engine asked to be read.
//! Shared with Fluree AI's hosted extraction, same ladder and floor.

use crate::{DocError, Result};
use image::{DynamicImage, ImageFormat};
use std::borrow::Cow;
use std::io::Cursor;

/// Binary bytes past which a crop is downscaled: the 6 MB request cap
/// less base64 inflation and the rest of the body.
pub const MAX_CROP_BYTES: usize = 4_200_000;

/// Long edge of the first attempt: above the ~1568 px the vision API
/// normalises to, so a crop landing here still supplies every pixel the
/// model will read.
const FIRST_LONG_EDGE: u32 = 2048;
/// Below this the crop is no longer the thing the engine selected, and a
/// bad transcription spliced in silently is worse than a refused crop.
const FLOOR_LONG_EDGE: u32 = 1024;
/// Keeps thin strokes and small type legible under reduction.
const FILTER: image::imageops::FilterType = image::imageops::FilterType::Lanczos3;

pub struct Fitted<'a> {
    pub bytes: Cow<'a, [u8]>,
    pub mime: &'a str,
}

/// Size and mime, never the pixels.
impl std::fmt::Debug for Fitted<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fitted")
            .field("bytes", &self.bytes.len())
            .field("borrowed", &matches!(self.bytes, Cow::Borrowed(_)))
            .field("mime", &self.mime)
            .finish()
    }
}

/// `image` unchanged when it fits in `cap`; otherwise decoded and
/// re-encoded progressively smaller until it does.
pub fn fit_under_cap<'a>(image: &'a [u8], mime: &'a str, cap: usize) -> Result<Fitted<'a>> {
    fit_with_ladder(image, mime, cap, FIRST_LONG_EDGE, FLOOR_LONG_EDGE)
}

fn fit_with_ladder<'a>(
    image: &'a [u8],
    mime: &'a str,
    cap: usize,
    first_long_edge: u32,
    floor_long_edge: u32,
) -> Result<Fitted<'a>> {
    if image.len() <= cap {
        return Ok(Fitted {
            bytes: Cow::Borrowed(image),
            mime,
        });
    }
    let source_format = decode_format(mime)?;
    let decoded = image::load_from_memory_with_format(image, source_format).map_err(|e| {
        DocError::Parse(format!(
            "cannot decode the {mime} crop to downscale it: {e}"
        ))
    })?;
    let (source_w, source_h) = (decoded.width(), decoded.height());
    let source_long_edge = source_w.max(source_h);
    if source_long_edge == 0 {
        return Err(DocError::Parse(format!(
            "crop reports a zero dimension ({source_w}x{source_h})"
        )));
    }
    // A JPEG re-encoded as PNG can multiply in size; photographic input
    // keeps its format, everything else becomes PNG.
    let (out_format, out_mime) = match source_format {
        ImageFormat::Jpeg => (ImageFormat::Jpeg, "image/jpeg"),
        _ => (ImageFormat::Png, "image/png"),
    };
    // Never scale up: noise-like content can exceed the cap at modest
    // dimensions, and the answer there is to keep halving.
    let mut target = first_long_edge.min(source_long_edge);
    loop {
        let (w, h) = scaled_dimensions(source_w, source_h, target);
        let resized = if w == source_w && h == source_h {
            Cow::Borrowed(&decoded)
        } else {
            Cow::Owned(decoded.resize(w, h, FILTER))
        };
        let encoded = encode(resized.as_ref(), out_format)?;
        if encoded.len() <= cap {
            tracing::info!(
                from = image.len(),
                to = encoded.len(),
                dimensions = format!("{source_w}x{source_h} → {w}x{h}"),
                "downscaled an oversize crop to fit the request ceiling"
            );
            return Ok(Fitted {
                bytes: Cow::Owned(encoded),
                mime: out_mime,
            });
        }
        if target <= floor_long_edge {
            return Err(DocError::Model(format!(
                "crop is {} bytes and still {} bytes at {w}x{h} (from {source_w}x{source_h}); \
                 past the request ceiling of {cap} bytes, and smaller than {floor_long_edge}px \
                 it is no longer worth reading",
                image.len(),
                encoded.len()
            )));
        }
        target = (target / 2).max(floor_long_edge);
    }
}

fn decode_format(mime: &str) -> Result<ImageFormat> {
    match mime {
        "image/png" => Ok(ImageFormat::Png),
        "image/jpeg" => Ok(ImageFormat::Jpeg),
        "image/gif" => Ok(ImageFormat::Gif),
        "image/webp" => Ok(ImageFormat::WebP),
        other => Err(DocError::Model(format!(
            "crop is past the request ceiling and cannot be downscaled: {other} is not a format this reader decodes"
        ))),
    }
}

/// `(w, h)` fitted inside a `long_edge` box, aspect preserved, never below
/// 1 px on either axis.
fn scaled_dimensions(w: u32, h: u32, long_edge: u32) -> (u32, u32) {
    let source_long_edge = w.max(h);
    if source_long_edge <= long_edge {
        return (w, h);
    }
    let scale = f64::from(long_edge) / f64::from(source_long_edge);
    let scaled = |v: u32| ((f64::from(v) * scale).round() as u32).max(1);
    (scaled(w), scaled(h))
}

fn encode(img: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>> {
    let mut out = Cursor::new(Vec::new());
    let result = match format {
        // The JPEG encoder rejects alpha; a crop has none to lose.
        ImageFormat::Jpeg => DynamicImage::ImageRgb8(img.to_rgb8()).write_to(&mut out, format),
        _ => img.write_to(&mut out, format),
    };
    result.map_err(|e| DocError::Parse(format!("cannot re-encode the downscaled crop: {e}")))?;
    Ok(out.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgb};

    /// Noise compresses to about its raw size, so a small canvas is
    /// already over a small cap.
    fn noisy_png(w: u32, h: u32) -> Vec<u8> {
        let mut seed = 0x9E37_79B9u32;
        let img = ImageBuffer::from_fn(w, h, |_, _| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            Rgb([seed as u8, (seed >> 8) as u8, (seed >> 16) as u8])
        });
        let mut out = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(img)
            .write_to(&mut out, ImageFormat::Png)
            .unwrap();
        out.into_inner()
    }

    #[test]
    fn a_crop_that_fits_is_untouched() {
        let png = noisy_png(64, 48);
        let fitted = fit_under_cap(&png, "image/png", usize::MAX).unwrap();
        assert!(matches!(fitted.bytes, Cow::Borrowed(_)));
        assert_eq!(fitted.mime, "image/png");
    }

    #[test]
    fn an_oversize_crop_walks_the_ladder_down() {
        let png = noisy_png(400, 300);
        let cap = png.len() / 3;
        let fitted = fit_with_ladder(&png, "image/png", cap, 256, 64).unwrap();
        assert!(fitted.bytes.len() <= cap);
        let out = image::load_from_memory(&fitted.bytes).unwrap();
        assert!(out.width() < 400);
        assert_eq!(out.width() * 300 / 400, out.height(), "aspect kept");
    }

    #[test]
    fn a_crop_still_too_big_at_the_floor_is_refused() {
        let png = noisy_png(400, 300);
        let err = fit_with_ladder(&png, "image/png", 10, 256, 200).unwrap_err();
        assert!(err.to_string().contains("no longer worth reading"));
    }

    #[test]
    fn dimensions_scale_by_long_edge() {
        assert_eq!(scaled_dimensions(3168, 2448, 2048), (2048, 1583));
        assert_eq!(scaled_dimensions(100, 50, 2048), (100, 50));
        assert_eq!(scaled_dimensions(4000, 1, 2000), (2000, 1));
    }
}
