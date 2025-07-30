# Location Module

A Beckn-OCPI bridge module for searching and retrieving charging station locations.

## Overview

This module provides an end-to-end solution for processing Beckn search requests and converting them to OCPI location queries, then transforming the results back into Beckn-compatible responses.

## Features

- **Beckn-OCPI Bridge**: Seamless integration between Beckn and OCPI protocols
- **Location Search**: Find charging stations within specified radius
- **Protocol Translation**: Convert between Beckn and OCPI data formats
- **Environment Configuration**: Secure configuration via `.env` file

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   Create a `.env` file in the module root:
   ```env
   OCPI_BASE_URL=https://your-ocpi-server.com
   OCPI_TOKEN=your-ocpi-token
   ```

3. **Run the search**:
   ```bash
   python search.py
   ```
