import { Disc3, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';

interface EmptyStateProps {
  onStartScan: () => void;
  isScanning: boolean;
}

export function EmptyState({ onStartScan, isScanning }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center mb-6">
        <Disc3 className="w-8 h-8 text-primary" />
      </div>
      
      <h3 className="text-xl font-semibold text-foreground mb-2">
        No Duplicates Found
      </h3>
      
      <p className="text-muted-foreground max-w-md mb-6">
        Your library looks clean! Start a new scan to check for duplicate albums, 
        or the scan is still in progress.
      </p>

      <Button 
        onClick={onStartScan}
        disabled={isScanning}
        className="gap-1.5"
      >
        <RefreshCw className="w-4 h-4" />
        Start New Scan
      </Button>
    </div>
  );
}
