# Fabric AI Data Agent Reference Implementation

## Overview

Civic Signal is a synthetic public-procurement intelligence reference platform used to demonstrate enterprise Microsoft Fabric analytics and AI patterns.

This implementation demonstrates how a governed Power BI semantic model can be prepared for AI and used as the grounding layer for a Microsoft Fabric Data Agent.

All data used by Civic Signal is synthetic. This repository does not contain production procurement data, customer data, credentials, or proprietary commercial logic.

## Architecture

```text
Synthetic procurement data
        |
        v
Microsoft Fabric analytical layers
        |
        v
SM_CivicSignal_Intelligence
        |
        +-- Curated semantic model
        +-- Explicit measures
        +-- Governed dimensions
        +-- Relationships
        |
        v
Prep data for AI
        |
        +-- Simplified AI schema
        +-- AI instructions
        +-- Verified Answers
        |
        v
DA_CivicSignal_Intelligence
        |
        +-- Semantic-model grounding
        +-- Agent instructions
        +-- Standard runtime
        +-- Published configuration