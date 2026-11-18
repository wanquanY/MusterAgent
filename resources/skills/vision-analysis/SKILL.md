---
name: vision-analysis
title: Vision Analysis
description: Use when one or more authorized images must be analyzed with active-route-aware visual routing.
when_to_use: Use when conclusions require pixels from one or more authorized images.
required_tools:
  - vision.analyze@4
---

# Vision Analysis

Use this Skill only when conclusions require pixels from one or more authorized images.

## Image sources

- Use `content_id` values already attached to the current Run.
- On a device placement, import images from the authorized local workspace with `device.workspace.asset.import@1` first.
- On a cloud placement, import original images from the current workspace with `workspace.asset.import@1` first.
- On a mobile surface, use only images explicitly imported into the selected project or conversation. A photo permission or a local path is not a ContentRef; never scan the photo library or silently move a local project to the cloud.
- Never pass filesystem paths, URLs, data URLs, object-storage keys, or guessed identifiers to `vision_analyze`.

## Batch behavior

- Put every image needed for the same question into one ordered `images` array.
- Use `analysis_mode=joint` for comparison or reasoning across images.
- Use `analysis_mode=independent` when each image can be inspected separately.
- Within the frozen route capacity, send every image for the question in one Tool call. Never call the Tool once per image.
- On an auxiliary visual route, one logical Tool call may create the minimum ordered physical model partitions when the frozen route limit requires it.
- On a native visual route, one Tool mount must fit the active route. If the Tool returns `vision_input_limit`, retry only with capacity-bounded ordered subsets. If it returns `vision_joint_input_limit`, reduce the same-request comparison set; never claim a lossless joint comparison across separate requests.
- Preserve stable labels when the answer must distinguish images.

## Routing

- A vision-capable active route sees the authorized pixels in the existing task context.
- A non-vision active route uses the Run-pinned auxiliary vision model configured by the operator.
- The auxiliary visual model is not an Agent and returns only an observation.

State uncertainty, partial failures, and any degraded cross-batch joint context explicitly.
