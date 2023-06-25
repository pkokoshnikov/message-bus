package org.pak.messagebus.core;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

@Builder
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
@Getter
public class CronConfig {
    @Builder.Default
    String creatingPartitionsCron = "0 0 1 * * ?";
    @Builder.Default
    String cleaningPartitionsCron = "0 0 2 * * ?";
}
