package org.pak.messagebus.core;

import java.time.LocalDate;

public record Partition(String name, LocalDate date) {}
