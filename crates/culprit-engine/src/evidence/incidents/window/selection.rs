//! Candidate times only bound discovery. Exact immutable native clocks select workload; older catalogs remain context.
use super::*;
use culprit_evidence::analytical::evidence::PublishedResourceContent;

pub(super) async fn discover(
    assembly: &mut Assembly<'_>,
    request: &WarehouseWindowQuery,
    gaps: &mut Vec<WarehouseWindowGap>,
) -> Result<(bool, Option<PublishedResourceCursor>)> {
    let published = request.view == WarehouseWindowView::PublishedAnswers;
    let content = if published { PublishedResourceContent::DerivedDiagnosis } else { PublishedResourceContent::All };
    let mut selection = PublishedResourceRequest {
        as_of: request.as_of.clone(),
        window: Some(request.window.clone()),
        after: request.after.clone(),
        page_rows: if published { request.page_rows } else { MAX_PUBLISHED_RESOURCE_PAGE_ROWS },
    };
    let mut complete = true;
    loop {
        let SelectedPublications { report, metadata } = assembly
            .session
            .selected_resources(
                &selection,
                Some(request.resource.database.clone()),
                PublishedResourceClock::NativeWindowCandidates,
                content,
            )
            .await?;
        let unavailable =
            report.gaps.iter().filter(|gap| **gap != PublishedResourceGap::MoreCandidates).cloned().collect::<Vec<_>>();
        if !unavailable.is_empty() {
            complete = false;
            gaps.push(WarehouseWindowGap::PublicationDiscovery { gaps: unavailable });
        }
        for metadata in metadata {
            let range = metadata.range_id().to_owned();
            if let Some(known) = assembly.metadata.get(&range) {
                if known.capture() != metadata.capture()
                    || known.capture_manifest_sha256() != metadata.capture_manifest_sha256()
                    || known.source() != metadata.source()
                    || known.collected_from() != metadata.collected_from()
                    || known.collected_until() != metadata.collected_until()
                {
                    return Err(invalid());
                }
            } else if assembly.metadata.len() + assembly.captures.len() >= assembly.capture_limit {
                gaps.push(WarehouseWindowGap::OriginalLimit);
                return Ok((false, None));
            } else {
                assembly.metadata.insert(range, metadata);
            }
        }
        if published {
            if report.next.is_some() {
                gaps.push(WarehouseWindowGap::MorePublishedAnalyses);
            }
            if assembly.metadata.is_empty() {
                gaps.push(WarehouseWindowGap::NoPublishedAnalyses);
            }
            return Ok((complete && report.next.is_none(), report.next));
        }
        let Some(next) = report.next else { break };
        if selection.after.as_ref().is_some_and(|previous| next.position <= previous.position) {
            return Err(DeliveryProblem::Conflict.error());
        }
        selection.after = Some(next);
    }
    if assembly.metadata.is_empty() {
        gaps.push(WarehouseWindowGap::NoPublishedOriginals);
    }
    Ok((complete, None))
}

pub(super) async fn read_originals(
    assembly: &mut Assembly<'_>,
    request: &WarehouseWindowQuery,
    gaps: &mut Vec<WarehouseWindowGap>,
) -> Result<Vec<WindowOriginal>> {
    let selections = assembly
        .metadata
        .values()
        .map(|metadata| {
            Ok((metadata.range_id().to_owned(), metadata_reference(metadata)?, families(metadata.source())))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut originals = Vec::new();
    for (range, original, families) in selections {
        if families.is_empty() {
            gaps.push(WarehouseWindowGap::UnsupportedOriginal { original });
            continue;
        }
        let Some(capture) = assembly.capture(&range, &families).await? else {
            gaps.push(WarehouseWindowGap::OriginalLimit);
            break;
        };
        let native_clock = WindowNativeClock::from_original(capture.source());
        if native_clock.overlaps(&request.window)?.is_none() && capture.derived_description().is_none() {
            gaps.push(WarehouseWindowGap::UnplacedNativeClock { original: original.clone() });
        }
        if !capture.is_read_complete() {
            gaps.push(WarehouseWindowGap::OriginalUnavailable {
                original: original.clone(),
                gaps: capture.gaps().to_vec(),
            });
        }
        originals.push(WindowOriginal {
            original,
            collection: capture.source().observation.collection(),
            native_clock,
            read_complete: capture.is_read_complete(),
        });
    }
    Ok(originals)
}
