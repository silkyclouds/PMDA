import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

/** Table row skeleton for loading states */
export function TableRowSkeleton({ columns = 5, className }: { columns?: number; className?: string }) {
  return (
    <div className={cn("flex items-center gap-4 p-4 border-b border-border", className)}>
      {Array.from({ length: columns }).map((_, i) => (
        <Skeleton key={i} className={cn("h-4", i === 0 ? "w-32" : "flex-1")} />
      ))}
    </div>
  );
}

/** Multiple table rows skeleton */
export function TableSkeleton({ rows = 5, columns = 5, className }: { rows?: number; columns?: number; className?: string }) {
  return (
    <div className={cn("rounded-lg border border-border overflow-hidden", className)}>
      {/* Header */}
      <div className="flex items-center gap-4 p-4 bg-muted/50 border-b border-border">
        {Array.from({ length: columns }).map((_, i) => (
          <Skeleton key={i} className={cn("h-3", i === 0 ? "w-24" : "flex-1")} />
        ))}
      </div>
      {/* Rows */}
      {Array.from({ length: rows }).map((_, i) => (
        <TableRowSkeleton key={i} columns={columns} />
      ))}
    </div>
  );
}

/** Album card skeleton for grid views */
export function AlbumCardSkeleton({ className }: { className?: string }) {
  return (
    <div className={cn("rounded-xl border border-border bg-card p-4 space-y-3", className)}>
      {/* Cover */}
      <Skeleton className="aspect-square rounded-lg w-full" />
      {/* Title */}
      <Skeleton className="h-4 w-3/4" />
      {/* Artist */}
      <Skeleton className="h-3 w-1/2" />
      {/* Meta */}
      <div className="flex gap-2">
        <Skeleton className="h-5 w-12 rounded-full" />
        <Skeleton className="h-5 w-16 rounded-full" />
      </div>
    </div>
  );
}

/** Grid of album cards skeleton */
export function AlbumGridSkeleton({ count = 8, className }: { count?: number; className?: string }) {
  return (
    <div className={cn("grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4", className)}>
      {Array.from({ length: count }).map((_, i) => (
        <AlbumCardSkeleton key={i} />
      ))}
    </div>
  );
}

/** Stats card skeleton */
export function StatsCardSkeleton({ className }: { className?: string }) {
  return (
    <div className={cn("rounded-xl border border-border bg-card p-5 space-y-2", className)}>
      <Skeleton className="h-3 w-20" />
      <Skeleton className="h-8 w-24" />
      <Skeleton className="h-3 w-16" />
    </div>
  );
}

/** Stats grid skeleton */
export function StatsGridSkeleton({ count = 4, className }: { count?: number; className?: string }) {
  return (
    <div className={cn("grid grid-cols-2 md:grid-cols-4 gap-4", className)}>
      {Array.from({ length: count }).map((_, i) => (
        <StatsCardSkeleton key={i} />
      ))}
    </div>
  );
}

/** Artist list item skeleton */
export function ArtistItemSkeleton({ className }: { className?: string }) {
  return (
    <div className={cn("flex items-center gap-3 p-3 rounded-lg", className)}>
      <Skeleton className="h-10 w-10 rounded-full" />
      <div className="flex-1 space-y-2">
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-3 w-20" />
      </div>
    </div>
  );
}

/** Artist sidebar skeleton */
export function ArtistListSkeleton({ count = 10, className }: { count?: number; className?: string }) {
  return (
    <div className={cn("space-y-1", className)}>
      {Array.from({ length: count }).map((_, i) => (
        <ArtistItemSkeleton key={i} />
      ))}
    </div>
  );
}

/** Page header skeleton */
export function PageHeaderSkeleton({ className }: { className?: string }) {
  return (
    <div className={cn("space-y-2", className)}>
      <Skeleton className="h-9 w-48" />
      <Skeleton className="h-4 w-72" />
    </div>
  );
}

/** Full page loading skeleton */
export function PageSkeleton({ 
  showHeader = true, 
  showStats = true, 
  showTable = true,
  className 
}: { 
  showHeader?: boolean; 
  showStats?: boolean; 
  showTable?: boolean;
  className?: string;
}) {
  return (
    <div className={cn("space-y-6 animate-fade-in", className)}>
      {showHeader && <PageHeaderSkeleton />}
      {showStats && <StatsGridSkeleton count={4} />}
      {showTable && <TableSkeleton rows={8} columns={5} />}
    </div>
  );
}
